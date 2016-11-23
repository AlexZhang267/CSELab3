package sdfs.client;

import sdfs.namenode.NameNodeServer;
import sdfs.namenode.SDFSFileChannelData;
import sdfs.protocol.IDataNodeProtocol;
import sdfs.protocol.INameNodeProtocol;

import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.AccessException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;

/**
 * Created by alex on 19/11/2016.
 */
public class SDFSClient implements ISDFSClient {
    static INameNodeProtocol iNameNodeProtocolImpl;
    static IDataNodeProtocol iDataNodeProtocolImpl;
    public static int FILE_DATA_BLOCK_CACHE_SIZE = 16;

    SDFSClient(){
        try {
            iNameNodeProtocolImpl = (INameNodeProtocol) Naming.lookup("rmi://localhost:12313/NameNodeServer");
            iDataNodeProtocolImpl = (IDataNodeProtocol)Naming.lookup("rmi://localhost:12314/DataNodeServer");
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }

    SDFSClient(int cacheSize,Registry registry){
        try{
            iNameNodeProtocolImpl = (INameNodeProtocol) registry.lookup("NameNodeServer");
            iDataNodeProtocolImpl = (IDataNodeProtocol) registry.lookup("DataNodeServer");
        } catch (RemoteException | NotBoundException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        SDFSClient SDFSClient = new SDFSClient();
        try {
            INameNodeProtocol i = iNameNodeProtocolImpl;
            i.create("test");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws IOException {
        SDFSFileChannelData sdfsFileChannelData = iNameNodeProtocolImpl.openReadonly(fileUri);
        return new SDFSFileChannel(sdfsFileChannelData,true);
    }

    @Override
    public SDFSFileChannel openReadWrite(String fileUri) throws IOException {
        SDFSFileChannelData sdfsFileChannelData = iNameNodeProtocolImpl.openReadwrite(fileUri);
        return new SDFSFileChannel(sdfsFileChannelData,false);
    }

    @Override
    public SDFSFileChannel create(String fileUri) throws IOException {
        SDFSFileChannelData sdfsFileChannelData = iNameNodeProtocolImpl.create(fileUri);
        SDFSFileChannel sdfsFileChannel = new SDFSFileChannel(sdfsFileChannelData,true);
        sdfsFileChannel.setReadOnly(false);
        return sdfsFileChannel;
    }

    @Override
    public void mkdir(String fileUri) throws IOException {
        iNameNodeProtocolImpl.mkdir(fileUri);
    }

    public void printNodes(){
        try {
            iNameNodeProtocolImpl.printNodes();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}
