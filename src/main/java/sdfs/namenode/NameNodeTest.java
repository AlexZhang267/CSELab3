package sdfs.namenode;

import sun.security.x509.GeneralNameInterface;

import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Created by alex on 19/11/2016.
 */
public class NameNodeTest {
    public static void main(String[] args) {
        try {
            Registry registry = LocateRegistry.createRegistry(12313);
            NameNodeServer nameNodeServer = new NameNodeServer(10,registry);
//            Naming.bind("rmi://localhost:12313/NameNodeServer",nameNodeServer);
            System.out.println("successfully");
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (AlreadyBoundException e) {
            e.printStackTrace();
        }
    }
}

