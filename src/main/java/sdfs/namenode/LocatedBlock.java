/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.namenode;

import java.io.Serializable;
import java.net.InetAddress;

public class LocatedBlock implements Serializable {
    private static final long serialVersionUID = -6509598325324530684L;
    private final InetAddress inetAddress;
    private final int dataBlockNumber;

    LocatedBlock(InetAddress inetAddress, int blockNumber) {
        if (inetAddress == null) {
            throw new NullPointerException();
        }
        this.inetAddress = inetAddress;
        this.dataBlockNumber = blockNumber;
    }

    public InetAddress getInetAddress() {
        return inetAddress;
    }

    public int getDataBlockNumber() {
        return dataBlockNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocatedBlock that = (LocatedBlock) o;

        return dataBlockNumber == that.dataBlockNumber && inetAddress.equals(that.inetAddress);
    }

    @Override
    public int hashCode() {
        int result = inetAddress.hashCode();
        result = 31 * result + dataBlockNumber;
        return result;
    }
}
