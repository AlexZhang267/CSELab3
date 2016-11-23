/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.filetree;

import java.io.Serializable;
import java.util.*;

public class FileNode implements Node, Serializable, Iterable<BlockInfo>, Cloneable {
    private static final long serialVersionUID = -5007570814999866661L;
    private List<BlockInfo> blockInfos = new ArrayList<>();
    private int fileSize;//file size should be checked when closing the file.

    public void addBlockInfo(BlockInfo blockInfo) {
        blockInfos.add(blockInfo);
    }

    public void removeLastBlockInfo() {
        blockInfos.remove(blockInfos.size() - 1);
    }

    public int getFileSize() {
        return fileSize;
    }

    public void setFileSize(int fileSize) {
        this.fileSize = fileSize;
    }

    @Override
    public Object clone(){
        Object o=null;
        try{
            o = (FileNode)super.clone();
            List<BlockInfo> blockInfoList=new ArrayList<>();
            for (BlockInfo blockInfo:blockInfos){
                blockInfoList.add((BlockInfo) blockInfo.clone());
            }
            ((FileNode)o).setBlockInfos(blockInfoList);
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return o;
    }


    @Override
    public Iterator<BlockInfo> iterator() {
        return blockInfos.listIterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileNode that = (FileNode) o;

        return blockInfos.equals(that.blockInfos);
    }

    @Override
    public int hashCode() {
        return blockInfos.hashCode();
    }

    public int getBlockAmount(){

        return blockInfos.size();
    }
    public BlockInfo getBlockInfo(int index) {
        if (index < blockInfos.size())
            return blockInfos.get(index);
        else {
            return null;
        }
    }

    public List<Integer> getAllowBlocks(){
        List<Integer> hashSet = new ArrayList<>();
        for (BlockInfo blockInfo:this){
            hashSet.add(blockInfo.getBlockIndex());
        }
        return hashSet;
    }

    public void setBlockInfos(List<BlockInfo> blockInfos) {
        this.blockInfos = blockInfos;
    }
}

