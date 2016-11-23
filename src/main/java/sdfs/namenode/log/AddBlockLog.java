package sdfs.namenode.log;

import sdfs.namenode.LocatedBlock;

import java.util.UUID;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class AddBlockLog implements Log {
    private UUID fileAccessToken;
    private LocatedBlock locatedBlock;

    public AddBlockLog(UUID fileAccessToken, LocatedBlock locatedBlock) {
        this.fileAccessToken = fileAccessToken;
        this.locatedBlock = locatedBlock;
    }

    public UUID getFileAccessToken() {
        return fileAccessToken;
    }

    public void setFileAccessToken(UUID fileAccessToken) {
        this.fileAccessToken = fileAccessToken;
    }

    public LocatedBlock getLocatedBlock() {
        return locatedBlock;
    }

    public void setLocatedBlock(LocatedBlock locatedBlock) {
        this.locatedBlock = locatedBlock;
    }
}
