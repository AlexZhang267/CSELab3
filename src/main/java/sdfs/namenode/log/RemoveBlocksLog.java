package sdfs.namenode.log;

import java.util.UUID;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class RemoveBlocksLog implements Log {
    private UUID fileAccessToken;
    private int blockAmount;

    public RemoveBlocksLog(UUID fileAccessToken) {
        this.fileAccessToken = fileAccessToken;
    }

    public UUID getFileAccessToken() {
        return fileAccessToken;
    }

    public void setFileAccessToken(UUID fileAccessToken) {
        this.fileAccessToken = fileAccessToken;
    }

    public int getBlockAmount() {
        return blockAmount;
    }

    public void setBlockAmount(int blockAmount) {
        this.blockAmount = blockAmount;
    }
}
