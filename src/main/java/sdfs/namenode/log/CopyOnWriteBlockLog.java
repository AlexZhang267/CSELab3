package sdfs.namenode.log;

import sdfs.namenode.LocatedBlock;

import java.util.UUID;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class CopyOnWriteBlockLog implements Log {
    private UUID fileAccessToken;
    private int fileBlockNumber;
    private LocatedBlock locatedBlock;

    public CopyOnWriteBlockLog(UUID fileAccessToken, int fileBlockNumber, LocatedBlock locatedBlock) {
        this.fileAccessToken = fileAccessToken;
        this.fileBlockNumber = fileBlockNumber;
        this.locatedBlock = locatedBlock;
    }

    public UUID getFileAccessToken() {
        return fileAccessToken;
    }

    public void setFileAccessToken(UUID fileAccessToken) {
        this.fileAccessToken = fileAccessToken;
    }

    public int getFileBlockNumber() {
        return fileBlockNumber;
    }

    public void setFileBlockNumber(int fileBlockNumber) {
        this.fileBlockNumber = fileBlockNumber;
    }

    public LocatedBlock getLocatedBlock() {
        return locatedBlock;
    }

    public void setLocatedBlock(LocatedBlock locatedBlock) {
        this.locatedBlock = locatedBlock;
    }
}
