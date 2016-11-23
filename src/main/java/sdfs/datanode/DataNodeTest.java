package sdfs.datanode;

import sdfs.namenode.NameNodeServer;
import sdfs.protocol.INameNodeDataNodeProtocol;
import sdfs.protocol.INameNodeProtocol;

import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

/**
 * Created by alex on 20/11/2016.
 */
public class DataNodeTest {
    public static void main(String[] args) {
        try {
            INameNodeDataNodeProtocol iNameNodeProtocolImpl = (INameNodeDataNodeProtocol) Naming.lookup("rmi://localhost:12313/NameNodeServer");
            DataNodeServer dataNodeServer = new DataNodeServer(iNameNodeProtocolImpl);
            LocateRegistry.createRegistry(12314);
            Naming.bind("rmi://localhost:12314/DataNodeServer",dataNodeServer);
            System.out.println("successfully");
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (AlreadyBoundException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        }
    }
}
