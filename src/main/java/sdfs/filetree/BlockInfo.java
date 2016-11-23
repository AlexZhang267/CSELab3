/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.filetree;

import sdfs.namenode.LocatedBlock;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BlockInfo implements Serializable, Iterable<LocatedBlock>, Cloneable {
    private static final long serialVersionUID = 8712105981933359634L;
    private final List<LocatedBlock> locatedBlocks = new ArrayList<>();

    @Override
    public Object clone(){
        Object o = null;
        try{
            o = super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return o;
    }
    @Override
    public Iterator<LocatedBlock> iterator() {
        return locatedBlocks.iterator();
    }

    public boolean addLocatedBlock(LocatedBlock locatedBlock) {
        return locatedBlocks.add(locatedBlock);
    }

    public boolean removeLocatedBlock(LocatedBlock locatedBlock) {
        return locatedBlocks.remove(locatedBlock);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BlockInfo that = (BlockInfo) o;

        return locatedBlocks.equals(that.locatedBlocks);
    }

    @Override
    public int hashCode() {
        return locatedBlocks.hashCode();
    }

    public int getBlockIndex(){
        if (locatedBlocks.size()>0){
            return locatedBlocks.get(0).getDataBlockNumber();
        }else{
            System.out.println("BlockInfo: getBlockIndex error, size = 0");
            return -1;
        }

    }
}
