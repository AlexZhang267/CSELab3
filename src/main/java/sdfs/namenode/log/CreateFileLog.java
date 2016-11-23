package sdfs.namenode.log;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class CreateFileLog implements Log {
    private String fileUri;

    public CreateFileLog(String fileUri) {
        this.fileUri = fileUri;
    }

    public String getFileUri() {
        return fileUri;
    }

    public void setFileUri(String fileUri) {
        this.fileUri = fileUri;
    }
}
