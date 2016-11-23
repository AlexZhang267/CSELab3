package sdfs.namenode;

import sdfs.client.SDFSFileChannel;
import sdfs.datanode.DataNodeServer;
import sdfs.exception.DirNotFoundException;
import sdfs.exception.IllegalAccessTokenException;
import sdfs.exception.SDFSFileAlreadyExistException;
import sdfs.filetree.BlockInfo;
import sdfs.filetree.DirNode;
import sdfs.filetree.Entry;
import sdfs.filetree.FileNode;
import sdfs.protocol.INameNodeDataNodeProtocol;
import sdfs.protocol.INameNodeProtocol;

import java.io.*;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.nio.channels.OverlappingFileLockException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


import static sdfs.utils.Config.DIR_NODE_FILE;
import static sdfs.utils.Config.LOCATED_BLOCK_NUM_FILE;

/**
 * Created by pengcheng on 2016/11/15.
 */

/**
 * extends UnicastRemoteObject
 * 如果不去掉这一句，在groovy中无法正常运行，去掉在java中运行则会出错
 */
public class NameNodeServer extends UnicastRemoteObject implements INameNodeProtocol, INameNodeDataNodeProtocol, Runnable, Serializable {
    private long flushDiskInternalSeconds;
    private static long FLUSH_DISK_INTERNAL_SECONDS = 10;
    public DirNode root;
    private int locatedBlockNum;

    private final Map<UUID, FileNode> readonlyFile = new HashMap<>();
    private final Map<UUID, FileNode> readwritePFile = new HashMap<>();
    private Map<UUID, AccessTokenPermission> tokenPermissionMap = new HashMap<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public NameNodeServer(long flushDiskInternalSeconds, Registry registry) throws RemoteException, AlreadyBoundException, MalformedURLException {
        super();
        readData();
        this.flushDiskInternalSeconds = flushDiskInternalSeconds;
        registry.bind("NameNodeServer", this);
        doShutDownWork();
    }

    private void doShutDownWork() {
        Runtime run = Runtime.getRuntime();
        run.addShutdownHook(new Thread() {
            @Override
            public void run() {
                //程序结束时将数据存入磁盘
                storeData();
            }
        });
    }

    @Override
    public void run() {

    }

    @Override
    public AccessTokenPermission getAccessTokenOriginalPermission(UUID fileAccessToken) throws RemoteException {
        return tokenPermissionMap.get(fileAccessToken);
    }

    @Override
    public Set<Integer> getAccessTokenNewBlocks(UUID fileAccessToken) throws RemoteException {
        return null;
    }

    @Override
    public SDFSFileChannelData openReadonly(String fileUri) throws IOException {
        String[] dirs = fileUri.split("/");
        DirNode cnode = root;//current node
        for (int i = 0; i < dirs.length - 1; i++) {
//            System.out.println(dirs[i]);
            Entry tmpEntry = cnode.searchEntry(dirs[i]);
            if (tmpEntry == null) {
                throw new FileNotFoundException();
            } else {
                cnode = (DirNode) tmpEntry.getNode();
            }
        }
        Entry entry = cnode.searchEntry(dirs[dirs.length - 1]);
        if (entry == null) {
            throw new FileNotFoundException();
        }
        if (entry.getNode() instanceof DirNode) {
            throw new FileNotFoundException("Cannot open a dir");
        }

        FileNode fileNode = (FileNode) entry.getNode();

        UUID uuid = UUID.randomUUID();
        readonlyFile.put(uuid, fileNode);
        AccessTokenPermission accessTokenPermission = new AccessTokenPermission(false, fileNode.getAllowBlocks());
        tokenPermissionMap.put(uuid, accessTokenPermission);
        return new SDFSFileChannelData(uuid, (FileNode) fileNode.clone());
    }

    @Override
    public SDFSFileChannelData openReadwrite(String fileUri) throws OverlappingFileLockException, IOException {
        String[] dirs = fileUri.split("/");
        DirNode cnode = root;//current node

        for (int i = 0; i < dirs.length - 1; i++) {
//            System.out.println(dirs[i]);
            Entry tmpEntry = cnode.searchEntry(dirs[i]);
            if (tmpEntry == null) {
                throw new FileNotFoundException();
            } else {
                cnode = (DirNode) tmpEntry.getNode();
            }
        }
        Entry entry = cnode.searchEntry(dirs[dirs.length - 1]);
        if (entry == null) {
            throw new FileNotFoundException();
        }
        FileNode fileNode = (FileNode) entry.getNode();

//        System.out.println("openreadwrite");
//        readwritePFile.keySet().forEach(System.out::println);
        for (Map.Entry<UUID, FileNode> map : readwritePFile.entrySet()) {
            if (fileNode == map.getValue()) {
                throw new OverlappingFileLockException();
            }
        }

        UUID uuid = UUID.randomUUID();
        AccessTokenPermission accessTokenPermission = new AccessTokenPermission(true, fileNode.getAllowBlocks());
        tokenPermissionMap.put(uuid, accessTokenPermission);
        readwritePFile.put(uuid, fileNode);
        return new SDFSFileChannelData(uuid, (FileNode) fileNode.clone());
    }

    @Override
    public SDFSFileChannelData create(String fileUri) throws IOException {
//        System.out.println(fileUri);
        String[] dirs = fileUri.split("/");
        DirNode cnode = root;//current node
        boolean tag = false;
        //需要之前的目录都存在
        //再将最后一个加上去
        for (int i = 0; i < dirs.length - 1; i++) {
//            System.out.println(dirs[i]);
            Entry tmpEntry = cnode.searchEntry(dirs[i]);
            if (tmpEntry == null) {
                tag = true;
                break;
            } else {
                cnode = (DirNode) tmpEntry.getNode();
            }
        }
        if (tag) {
            throw new FileNotFoundException();
        } else {

            Entry entry1 = cnode.searchEntry(dirs[dirs.length - 1]);
            if (entry1 != null) {
                throw new SDFSFileAlreadyExistException();
            }

            FileNode fileNode = new FileNode();
            UUID uuid2 = UUID.randomUUID();
            AccessTokenPermission accessTokenPermission = new AccessTokenPermission(true, new ArrayList<>());

            tokenPermissionMap.put(uuid2, accessTokenPermission);
            readwritePFile.put(uuid2, fileNode);
//            System.out.println("create");
//            readwritePFile.keySet().forEach(System.out::println);
            Entry entry = new Entry(dirs[dirs.length - 1], fileNode);
            cnode.addEntry(entry);
            return new SDFSFileChannelData(uuid2, (FileNode) fileNode.clone());
        }
    }

    @Override
    public void closeReadonlyFile(UUID fileAccessToken) throws IllegalAccessTokenException, IOException {
        if (!tokenPermissionMap.containsKey(fileAccessToken)){
            throw new IllegalAccessTokenException();
        }else{
            if (tokenPermissionMap.get(fileAccessToken).isWriteable()){
                throw new IllegalAccessTokenException();
            }
        }

        readonlyFile.remove(fileAccessToken);
        tokenPermissionMap.remove(fileAccessToken);
    }

    @Override
    public void closeReadwriteFile(UUID fileAccessToken, long newFileSize) throws IllegalAccessTokenException, IllegalArgumentException, IOException {
        //todo:更新filenode 的信息，更新过程中应该不允许其他进程访问这个filenode
        if (!tokenPermissionMap.containsKey(fileAccessToken)){
            throw new IllegalAccessTokenException();
        }else{
            if (!tokenPermissionMap.get(fileAccessToken).isWriteable()){
                throw new IllegalAccessTokenException();
            }
        }


        //上写锁
        lock.writeLock().lock();
        FileNode fileNode = readwritePFile.get(fileAccessToken);
        int blockAmount = tokenPermissionMap.get(fileAccessToken).getAllowBlocks().size();
//        System.out.println("blockamount: "+blockAmount);
        if (blockAmount==0 && newFileSize!=0){
            readwritePFile.remove(fileAccessToken);
            tokenPermissionMap.remove(fileAccessToken);
            throw new IllegalArgumentException();
        }
        if (!(newFileSize>(blockAmount-1)*DataNodeServer.BLOCK_SIZE && newFileSize <= blockAmount*DataNodeServer.BLOCK_SIZE)){
            readwritePFile.remove(fileAccessToken);
            tokenPermissionMap.remove(fileAccessToken);
            throw new IllegalArgumentException();
        }

        fileNode.setFileSize((int) newFileSize);
        readwritePFile.remove(fileAccessToken);
        AccessTokenPermission accessTokenPermission = tokenPermissionMap.get(fileAccessToken);
        fileNode.setBlockInfos(new ArrayList<>());
        for (int index : accessTokenPermission.getAllowBlocks()) {
            BlockInfo blockInfo = new BlockInfo();
            LocatedBlock locatedBlock = new LocatedBlock(InetAddress.getLocalHost(),index);
            blockInfo.addLocatedBlock(locatedBlock);
            fileNode.addBlockInfo(blockInfo);
        }
        tokenPermissionMap.remove(fileAccessToken);
        lock.writeLock().unlock();
    }

    @Override
    public void mkdir(String fileUri) throws IOException {
        if (fileUri.charAt(0) == '/') {
            fileUri = fileUri.substring(1);
        }
        String[] dirs = fileUri.split("/");
        DirNode cnode = root;//current node

        for (int i = 0; i < dirs.length - 1; i++) {
            Entry tmpEntry = cnode.searchEntry(dirs[i]);
            if (tmpEntry == null) {
                DirNode dirNode = new DirNode();
                Entry entry = new Entry(dirs[i], dirNode);
                cnode.addEntry(entry);
                cnode = (DirNode) entry.getNode();
            } else {
                cnode = (DirNode) tmpEntry.getNode();
            }
        }

        Entry lastEnrty = cnode.searchEntry(dirs[dirs.length - 1]);
        if (lastEnrty != null) {
            throw new SDFSFileAlreadyExistException();
        }
        DirNode dirNode = new DirNode();
        lastEnrty = new Entry(dirs[dirs.length - 1], dirNode);
        cnode.addEntry(lastEnrty);
    }


    public LocatedBlock addBlock(UUID fileUuid) {
        FileNode fileNode = readwritePFile.get(fileUuid);
        if (fileNode == null) {
            System.out.println("add block error");
        }
        try {
            //todo:located 暂时是localhost
            //todo: newCopyOnWriteBlock
            LocatedBlock locatedBlock = new LocatedBlock(InetAddress.getLocalHost(), locatedBlockNum);

            //每次addblcok之后需要将locatedBlockNum更新
            locatedBlockNum++;
            BlockInfo blockInfo = new BlockInfo();
            blockInfo.addLocatedBlock(locatedBlock);
            //todo: filenode不可以直接addblock

//            fileNode.addBlockInfo(blockInfo);
            // 在这个uuid的allow block中添加新的块
            AccessTokenPermission accessTokenPermission = tokenPermissionMap.get(fileUuid);
            int blockIndex = blockInfo.getBlockIndex();
            accessTokenPermission.addBlock(blockIndex);

            return locatedBlock;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<LocatedBlock> addBlocks(UUID fileAccessToken, int blockAmount) throws IllegalAccessTokenException, RemoteException {

        if (!readwritePFile.containsKey(fileAccessToken)){
            throw new IllegalAccessTokenException();
        }
        if (blockAmount<0){
            throw new IllegalArgumentException();
        }

        List<LocatedBlock> locatedBlocks = new ArrayList<>();
        try {
            for (int i = 0; i < blockAmount; i++) {
                LocatedBlock locatedBlock = new LocatedBlock(InetAddress.getLocalHost(), locatedBlockNum);
                BlockInfo blockInfo = new BlockInfo();
                blockInfo.addLocatedBlock(locatedBlock);
                AccessTokenPermission accessTokenPermission = tokenPermissionMap.get(fileAccessToken);
                if (accessTokenPermission==null){
                    throw new IllegalAccessTokenException();
                }
                int blockIndex = blockInfo.getBlockIndex();
                accessTokenPermission.addBlock(blockIndex);
                // 添加到copyOnWriteMap中去
//                copyOnWriteMap.put(blockIndex,blockIndex);
                locatedBlocks.add(locatedBlock);
                locatedBlockNum++;
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return locatedBlocks;
    }


    @Override
    public void removeLastBlocks(UUID fileAccessToken, int blockAmount) throws IllegalAccessTokenException, IndexOutOfBoundsException, RemoteException {
        if (!readwritePFile.containsKey(fileAccessToken)){
            throw new IllegalAccessTokenException();
        }
        if (blockAmount<0){
            throw new IllegalArgumentException();
        }

        AccessTokenPermission accessTokenPermission = tokenPermissionMap.get(fileAccessToken);
        int size = accessTokenPermission.getAllowBlocks().size();
        for (int i = 0; i < blockAmount; i++) {
            accessTokenPermission.getAllowBlocks().remove(size-1);
            size--;
        }
    }

    @Override
    public LocatedBlock newCopyOnWriteBlock(UUID fileAccessToken, int fileBlockNumber) throws IllegalStateException, RemoteException {
//        System.out.println("readwritePFile");
//        readwritePFile.keySet().forEach(System.out::println);
//        System.out.println("newCopyOnWriteBlock: "+fileAccessToken);
        if (!readwritePFile.containsKey(fileAccessToken)){
            throw new IllegalAccessTokenException();
        }
        AccessTokenPermission accessTokenPermission = tokenPermissionMap.get(fileAccessToken);

        if (fileBlockNumber<0||fileBlockNumber>=accessTokenPermission.getAllowBlocks().size()){
            throw new IndexOutOfBoundsException();
        }

        int blockIndex = accessTokenPermission.getAllowBlocks().get(fileBlockNumber);

        int newBlockIndex = locatedBlockNum;
        locatedBlockNum++;

        // 移除第fileblockNumber个block
        // todo: 加锁
        accessTokenPermission.getAllowBlocks().remove(fileBlockNumber);
        //在同一个位置上设为新的block index
        accessTokenPermission.getAllowBlocks().add(fileBlockNumber,newBlockIndex);

//        copyOnWriteMap.put(fileBlockNumber,newBlockIndex);

        LocatedBlock locatedBlock = null;
        try {
            locatedBlock= new LocatedBlock(InetAddress.getLocalHost(), newBlockIndex);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        return locatedBlock;
    }


    private void readData() {
        DirNode tmpDirNode = readRootDirNode();
        if (tmpDirNode == null) {
            System.out.println("ERROR: readRootDirNode");
            root = new DirNode();
        } else {
            root = tmpDirNode;
        }

        locatedBlockNum = readLocatedBlockNum();
    }

    private DirNode readRootDirNode() {
        File file = new File(DIR_NODE_FILE);
        if (!file.exists()) {
            return new DirNode();
        }
        try {
            ObjectInputStream is = new ObjectInputStream(
                    new FileInputStream(DIR_NODE_FILE));
            DirNode dirNode = (DirNode) is.readObject();
            is.close();
            return dirNode;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    private int readLocatedBlockNum() {
        File file = new File(LOCATED_BLOCK_NUM_FILE);
        if (!file.exists()) {
            return 0;
        }
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(LOCATED_BLOCK_NUM_FILE)));
            System.out.println("read from file");
            return br.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }


    private void storeData() {
        storeDirNode();
        storeLocatedBlockNum();
    }

    //todo: 方法名改为storeNode
    private void storeDirNode() {
        try {
            ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(DIR_NODE_FILE));
            os.writeObject(root);
            os.flush();
            os.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void storeLocatedBlockNum() {
        try {
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(LOCATED_BLOCK_NUM_FILE)));
            bw.write(locatedBlockNum);
            bw.flush();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /*
    辅助打印
     */
    @Override
    public void printNodes() {
        System.out.println("print dir tree");
        printDir(0, root);
    }

    private void printDir(int space, DirNode node) {
        for (Entry entry : node) {
            for (int i = 0; i < space; i++) {
                System.out.print("  ");
            }
            System.out.print(">");
            System.out.println(entry.getName());
            if (entry.getNode() instanceof DirNode) {
                printDir(space + 1, (DirNode) entry.getNode());
            }
        }

    }
}
