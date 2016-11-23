/**
 * Created by alex on 19/11/2016.
 */

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class HelloImpl extends UnicastRemoteObject implements Hello {
    private static final long serialVersionUID = -271947229644133464L;

    public HelloImpl() throws RemoteException {
        super();
    }

    @Override
    public String sayHello(String name) throws RemoteException {
        return "Hello," + name;
    }
}
