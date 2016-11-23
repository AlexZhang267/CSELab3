import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Created by alex on 19/11/2016.
 */
public interface Hello extends Remote{
    public String sayHello(String msg) throws RemoteException;
}
