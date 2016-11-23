package sdfs.datanode;

import sdfs.exception.IllegalAccessTokenException;
import sdfs.namenode.AccessTokenPermission;
import sdfs.protocol.IDataNodeProtocol;
import sdfs.protocol.INameNodeDataNodeProtocol;
import sdfs.utils.Config;

import java.io.*;
import java.nio.channels.FileChannel;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.UUID;

import static sdfs.utils.Config.DATANODESPATH;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class DataNodeServer extends UnicastRemoteObject implements IDataNodeProtocol, Serializable {
    public static int BLOCK_SIZE = 64 * 1024;
    private INameNodeDataNodeProtocol data2name;

    public DataNodeServer(INameNodeDataNodeProtocol data2name) throws RemoteException {
        super();
        this.data2name = data2name;
    }

    @Override
    public byte[] read(UUID fileAccessToken, int blockNumber, long position, int size) throws IllegalAccessTokenException, IllegalArgumentException, IOException {
        AccessTokenPermission accessTokenPermission = data2name.getAccessTokenOriginalPermission(fileAccessToken);

        if (accessTokenPermission == null || !accessTokenPermission.getAllowBlocks().contains(blockNumber)) {
            throw new IllegalAccessTokenException();
        }

        File file = new File(DATANODESPATH + blockNumber + ".block");

        if (!file.exists()) {
            //  找不到请求的datanode时就抛异常，server会根据异常包装一个状态为错误的response发送到客户端
//            throw new FileNotFoundException();
            return new byte[0];
        }

        if (position < 0 || position + size > BLOCK_SIZE) {
            throw new IllegalArgumentException();
        }

        byte[] b = new byte[size];
        RandomAccessFile raf = new RandomAccessFile(DATANODESPATH + blockNumber+".block","r");
        raf.seek(position);
        raf.read(b,0,size);
        raf.close();
        return b;
    }

    @Override
    public void write(UUID fileAccessToken, int blockNumber, long position, byte[] buffer) throws IllegalAccessTokenException, IllegalArgumentException, IOException {
        AccessTokenPermission accessTokenPermission = data2name.getAccessTokenOriginalPermission(fileAccessToken);

        if (accessTokenPermission == null || !accessTokenPermission.getAllowBlocks().contains(blockNumber)) {
            throw new IllegalAccessTokenException();
        }

        if (position<0 || position+buffer.length>BLOCK_SIZE){
            throw new IllegalArgumentException();
        }

        File file = new File(DATANODESPATH + blockNumber + ".block");
        if (!file.exists()) {
            file.createNewFile();
        }

        // byte[] b, int off, int len
        // off 为b中的开始位置
        RandomAccessFile raf = new RandomAccessFile(DATANODESPATH+blockNumber+".block","rw");
        raf.seek(position);
        raf.write(buffer,0,buffer.length);
        raf.close();
    }
}
