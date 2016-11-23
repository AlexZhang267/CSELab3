package sdfs.namenode.log;

import java.util.UUID;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class WriteStartLog implements Log {
    private String fileUri;
    private UUID fileAccessToken;

    public WriteStartLog(String fileUri, UUID fileAccessToken) {
        this.fileUri = fileUri;
        this.fileAccessToken = fileAccessToken;
    }

    public String getFileUri() {
        return fileUri;
    }

    public void setFileUri(String fileUri) {
        this.fileUri = fileUri;
    }

    public UUID getFileAccessToken() {
        return fileAccessToken;
    }

    public void setFileAccessToken(UUID fileAccessToken) {
        this.fileAccessToken = fileAccessToken;
    }
}
