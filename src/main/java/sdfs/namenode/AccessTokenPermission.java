package sdfs.namenode;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class AccessTokenPermission implements Serializable {
    private static final long serialVersionUID = -6174811460052859447L;
    private boolean writeable;
//    private Set<Integer> allowBlocks;
    private List<Integer> allowBlocks;

    public AccessTokenPermission(boolean writeable, List<Integer> allowBlocks) {
        this.writeable = writeable;
        this.allowBlocks = allowBlocks;
    }

    public void addBlock(int blockIndex){
        allowBlocks.add(blockIndex);
    }

    public List<Integer> getAllowBlocks(){
        return allowBlocks;
    }

    public boolean isWriteable() {
        return writeable;
    }
}