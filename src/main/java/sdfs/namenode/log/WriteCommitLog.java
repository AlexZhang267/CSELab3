package sdfs.namenode.log;

import java.util.UUID;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class WriteCommitLog implements Log {
    private UUID fileAccessToken;
    private long newFileSize;

    public WriteCommitLog(UUID fileAccessToken, long newFileSize) {
        this.fileAccessToken = fileAccessToken;
        this.newFileSize = newFileSize;
    }

    public UUID getFileAccessToken() {
        return fileAccessToken;
    }

    public void setFileAccessToken(UUID fileAccessToken) {
        this.fileAccessToken = fileAccessToken;
    }

    public long getNewFileSize() {
        return newFileSize;
    }

    public void setNewFileSize(long newFileSize) {
        this.newFileSize = newFileSize;
    }
}
