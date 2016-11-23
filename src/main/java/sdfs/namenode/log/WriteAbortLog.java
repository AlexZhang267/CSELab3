package sdfs.namenode.log;

import java.util.UUID;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class WriteAbortLog implements Log {
    private UUID fileAccessToken;

    public WriteAbortLog(UUID fileAccessToken) {
        this.fileAccessToken = fileAccessToken;
    }

    public UUID getFileAccessToken() {
        return fileAccessToken;
    }

    public void setFileAccessToken(UUID fileAccessToken) {
        this.fileAccessToken = fileAccessToken;
    }
}
