/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.filetree.BlockInfo;
import sdfs.filetree.FileNode;
import sdfs.namenode.LocatedBlock;
import sdfs.namenode.SDFSFileChannelData;

import java.io.Flushable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.*;

public class SDFSFileChannel implements SeekableByteChannel, Flushable, Serializable {
    private static final long serialVersionUID = 6892411224902751501L;
    private boolean readOnly;
    private boolean opened;
    private int position;
    private int bufferSize = 64*1024;
    private int blockSize = 64*1024;
    private SDFSFileChannelData sdfsFileChannelData;

    private final Map<Integer, byte[]> dataBlocksCache = new HashMap<>(); //BlockNumber to DataBlock cache. byte[] or ByteBuffer are both acceptable.
    private final Map<Integer, Boolean> bufferDirty = new HashMap<>();//表示第几个块是否是脏的
    private final List<Integer> LRUList = new ArrayList<>();
    private final Map<Integer,Integer> copyOnWriteMap = new HashMap<>();


    private int cacheLimit = 16;

    SDFSFileChannel(SDFSFileChannelData sdfsFileChannelData, boolean readOnly){
        this.sdfsFileChannelData =sdfsFileChannelData;
        this.readOnly = readOnly;
        opened = true;
        position = 0;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        //todo your code here
        if (!opened) {
            throw new ClosedChannelException();
        }

        int dstPosition = dst.position();
        int readSize = dst.capacity() - dstPosition;
        int fileSize = getFileNode().getFileSize() - this.position;

        if (fileSize <= 0) {
            return 0;
        }

        //最多可以读取的byte数组的大小
        int minimum = Math.min(readSize, fileSize);


        //filesize == 0 没有block 不需要load
        if (getFileSize()==0){
            return 0;
        }

        int blockIndex = (position-1) / bufferSize;//当前需要的block
        // 如果没有当前的block
        // 就从datanode中将需要的block load 进来
        if (!haveBlock(blockIndex)) {
            loadBlock(blockIndex);
        }


        byte[] buffer = dataBlocksCache.get(blockIndex);
        upBlock(blockIndex);

        int originBlockIndex = (position-1) / bufferSize;
        for (int i = 0; i < minimum; i++) {
            blockIndex = (position + i) / bufferSize;
            if (!haveBlock(blockIndex)) {
                loadBlock(blockIndex);
                buffer = dataBlocksCache.get(blockIndex);
                upBlock(blockIndex);
            }

            if (blockIndex!=originBlockIndex){
                originBlockIndex = blockIndex;
                buffer = dataBlocksCache.get(originBlockIndex);
            }

            dst.put(dstPosition + i, buffer[(position + i) % bufferSize]);
        }

        //读完之后更新fileCursor
        position += minimum;
        dst.position(dstPosition + minimum);

        return minimum;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        //todo your code here
        if (!opened) {
            throw new ClosedChannelException();
        }

        if (readOnly) {
            throw new NonWritableChannelException();
        }

        if (getFileSize() < position()) {
            setFileSize(position);
        }


        int srcPosition = src.position();
        int writeSize = src.capacity() - srcPosition;
        int requireBlockNum = (position + writeSize) / blockSize + 1;
        int blockNum = getFileNode().getBlockAmount();
        if (requireBlockNum > blockNum) {
            UUID uuid = getAccessToken();
            List<LocatedBlock> locatedBlockList =SDFSClient.iNameNodeProtocolImpl.addBlocks(uuid,requireBlockNum - blockNum);
            for (LocatedBlock locatedBlock : locatedBlockList) {
                BlockInfo blockInfo = new BlockInfo();
                blockInfo.addLocatedBlock(locatedBlock);
                FileNode fileNode = getFileNode();
                fileNode.addBlockInfo(blockInfo);
            }
        }

        int blockIndex = (position-1) / bufferSize;//当前需要的block
        if (!haveBlock(blockIndex)&&getFileSize()!=0) {
            loadBlock(blockIndex);
        }
        byte[] buffer = dataBlocksCache.get(blockIndex);
        upBlock(blockIndex);


        // 将这块cache标记为脏的
        for (int i = 0; i < writeSize; i++) {
            blockIndex = (position + i) / blockSize;
            //将当前正在写的块标记为脏
            // todo: 每次都写对性能影响大不大
            bufferDirty.put(blockIndex, true);
            if (!haveBlock(blockIndex)) {
                loadBlock(blockIndex);
                buffer = dataBlocksCache.get(blockIndex);
                upBlock(blockIndex);
            }
            buffer[(position + i) % blockSize] = src.get(srcPosition + i);
        }
//        fileSize += writeSize;
        addFileSize(writeSize);
        position += writeSize;
        src.position(srcPosition + writeSize);
        return writeSize;

    }

    @Override
    public long position() throws IOException {
        //todo your code here
//        return 0;
        if (!opened) {
            throw new ClosedChannelException();
        }
        return position;
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        //todo your code here
//        return null;
        if (!opened) {
            throw new ClosedChannelException();
        }
        if (newPosition < 0) {
            throw new IllegalArgumentException();
        }
        position = (int) newPosition;
        return this;
    }

    @Override
    public long size() throws IOException {
        //todo your code here
//        return 0;
        if (!opened) {
            throw new ClosedChannelException();
        }
        return getFileSize();
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        //todo your code here
        if (!opened) {
            throw new ClosedChannelException();
        }

        if (readOnly) {
            throw new NonWritableChannelException();
        }

        if (size > getFileSize()) {
            return this;
        }
        if (size < position()) {
            position(size);
        }

        // update lcoal filenode information
        getFileNode().setFileSize((int) size);
        if (position >= size) {
            position = (int) size;
        }

        int blockIndex = (position-1) / blockSize;

        //此时的block应该为0
        if (position==0){
            blockIndex=-1;
        }

        //clear cache
        List<Integer> dataBlockIndexs = new ArrayList();
        List<Integer> dirtyBlockIndexs = new ArrayList();
        int finalBlockIndex = blockIndex;
        dataBlocksCache.keySet().stream().filter(key -> key > finalBlockIndex).forEach(key -> {
            dataBlockIndexs.add(key);
            if (bufferDirty.keySet().contains(key)) {
                dirtyBlockIndexs.add(key);
            }
        });

        dataBlockIndexs.forEach(dataBlocksCache::remove);
        dirtyBlockIndexs.forEach(bufferDirty::remove);
        int offset = position % blockSize;
        byte[] buffer = dataBlocksCache.get(blockIndex);
        if (buffer != null) {
            for (int o = offset; o < blockSize; o++) {
                buffer[o] = 0x00000000;
            }
        }
        bufferDirty.put(blockIndex, true);

        int removedBlockAmount = getBlockAmount() - blockIndex - 1;


        //移除block 先移除本地的再移除远程的，这样的好处是
        // TODO: 21/11/2016
        int blockAmount = getBlockAmount();

        ArrayList<LocatedBlock> locatedBlocks = new ArrayList<>();
        for (int i = 0; i < removedBlockAmount; i++) {
            BlockInfo blockInfo = getFileNode().getBlockInfo(getFileNode().getBlockAmount() - 1);
            for (LocatedBlock lb : blockInfo) {
                locatedBlocks.add(lb);
            }
            getFileNode().removeLastBlockInfo();
        }

//        NameNodeStub.getNameNodeStub().removeLastBlocks(uuid, blockAmount - blockIndex - 1);
        SDFSClient.iNameNodeProtocolImpl.removeLastBlocks(getAccessToken(),blockAmount-blockIndex-1);
        //todo: update datanode
//        DataNodeStub.getDataNodeStub().removeblocks(locatedBlocks);
//        SDFSClient.iDataNodeProtocolImpl.
        return this;
    }

    @Override
    public boolean isOpen() {
        //todo your code here
        return opened;
    }

    @Override
    public void close() throws IOException {
        //todo your code here
        if (!opened) {
            return;
        }
        if (readOnly) {
            SDFSClient.iNameNodeProtocolImpl.closeReadonlyFile(getAccessToken());
        } else {
            flush();
            SDFSClient.iNameNodeProtocolImpl.closeReadwriteFile(getAccessToken(),getFileSize());
            //todo: 注释之后会有影响吗
//            fileNode.setFileSize(fileSize);
        }
        opened = false;
    }

    @Override
    public void flush() throws IOException {
        //todo your code here
        if (!opened) {
            throw new ClosedChannelException();
        }

        if (readOnly) {
            throw new NonWritableChannelException();
        }
        for (int key : dataBlocksCache.keySet()) {
            //找到所有脏的block flush掉
            if (bufferDirty.get(key)) {
                flushBlock(key);
            }
        }
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public FileNode getFileNode(){
        return sdfsFileChannelData.getFileNode();
    }

    public int getFileSize(){
        return getFileNode().getFileSize();
    }
    private void setFileSize(int n){
        getFileNode().setFileSize(n);
    }
    private void addFileSize(int n){
        int size = getFileSize();
        getFileNode().setFileSize(size+n);
    }


    public int getBlockAmount(){
        return getFileNode().getBlockAmount();
    }

    private boolean haveBlock(int index) {
        for (int key : dataBlocksCache.keySet()) {
            if (index == key) {
                return true;
            }
        }
        return false;
    }

    public UUID getAccessToken(){
        return sdfsFileChannelData.getAccessToken();
    }
    //将index号block load到目前可用的一个buffer中
    private void loadBlock(int index) {
        byte[] buffer = getAvailableBuffer(index);
        if (buffer == null) {
            buffer = new byte[blockSize];
        }
        BlockInfo blockInfo = getFileNode().getBlockInfo(index);
        int blockNum = 0;
        for (LocatedBlock locatedBlock : blockInfo) {
            //todo: 只考虑了一个locatedblock的情况
            blockNum = locatedBlock.getDataBlockNumber();
        }
        try {
            byte[] res = SDFSClient.iDataNodeProtocolImpl.read(getAccessToken(),blockNum,0,blockSize);
            if (res != null) {
                for (int i = 0; i < res.length; i++) {
                    buffer[i] = res[i];
                }
            }
        } catch (IOException e) {
            System.out.println("datanode dont have this block: " + index);
        }
    }
    public byte[] getAvailableBuffer(int index) {
        // 到达cache的上限就删除最前面的一个buffer
        if (dataBlocksCache.size() == cacheLimit) {
            int removeIndex = LRUList.remove(0);
            if (bufferDirty.get(removeIndex)) {
                if (bufferDirty.get(removeIndex)) {
                    try {
                        flushBlock(removeIndex);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            dataBlocksCache.remove(removeIndex);
            bufferDirty.remove(removeIndex);
        }

        byte[] newBuffer = new byte[bufferSize];
        LRUList.add(index);
        dataBlocksCache.put(index, newBuffer);
        bufferDirty.put(index, false);
        return newBuffer;
    }

    /**
     * @param blockIndex flush指定的block
     * @throws IOException
     */
    private void flushBlock(int blockIndex) throws IOException {
        int lastBlockIndex = (getFileSize() - 1) / blockSize;

        // 需要写这一个block
        //blockIndex指的上第几个块
        //getDataBlockNumber指的是那个locatedBlock里的存放的datanode里的block的编号

        System.out.println("flushBlock: blockIndex" + blockIndex);
        System.out.println("flushBlock: actual block number " + getFileNode().getAllowBlocks().get(blockIndex));
        LocatedBlock cowBlock = SDFSClient.iNameNodeProtocolImpl.newCopyOnWriteBlock(getAccessToken(),blockIndex);
        System.out.println("flushBlock: cowBlockIndex" + cowBlock.getDataBlockNumber());
        copyOnWriteMap.put(getFileNode().getAllowBlocks().get(blockIndex),cowBlock.getDataBlockNumber());

        if (lastBlockIndex == blockIndex) {
            int end = (getFileSize() - 1) % blockSize;
            byte[] buffer1 = new byte[end + 1];
            BlockInfo blockInfo = getFileNode().getBlockInfo(blockIndex);
            byte[] buffer2 = dataBlocksCache.get(blockIndex);
            for (int i = 0; i < end + 1; i++) {
                buffer1[i] = buffer2[i];
            }
            for (LocatedBlock locatedBlock : blockInfo) {
                int locatedBlockNumber = locatedBlock.getDataBlockNumber();
                //datanode那边写的其实是copy的块
                System.out.println("flushBlock: locatedBlockNumber "+locatedBlockNumber);
                System.out.println("flushBlock: locatedBlockNumber mapping "+copyOnWriteMap.get(locatedBlockNumber));
                SDFSClient.iDataNodeProtocolImpl.write(getAccessToken(),copyOnWriteMap.get(locatedBlockNumber),0,buffer1);
            }
        } else {
            BlockInfo blockInfo = getFileNode().getBlockInfo(blockIndex);
            byte[] buffer = dataBlocksCache.get(blockIndex);
            int locatedBlockNumber = 0;
            for (LocatedBlock locatedBlock : blockInfo) {
                locatedBlockNumber = locatedBlock.getDataBlockNumber();
                System.out.println("flushBlock: locatedBlockNumber "+locatedBlockNumber);
                System.out.println("flushBlock: locatedBlockNumber mapping "+copyOnWriteMap.get(locatedBlockNumber));
                SDFSClient.iDataNodeProtocolImpl.write(getAccessToken(),copyOnWriteMap.get(locatedBlockNumber),0,buffer);
            }
        }
    }

    //将blockIndex对应的buffer在LRUList中向后移
    private void upBlock(int blockIndex) {
        for (int i = LRUList.size() - 1; i >= 0; i--) {
            if (LRUList.get(i) == blockIndex) {
                LRUList.remove(i);
                LRUList.add(blockIndex);
                break;
            }
        }
    }
}
