package com.jivesoftware.os.miru.service.index;

/**
 *
 */
public class MiruFieldIndexKey {

    private final int id;
    private int maxId;

    public MiruFieldIndexKey(int id, int maxId) {
        this.id = id;
        this.maxId = maxId;
    }

    public MiruFieldIndexKey(int id) {
        this(id, -1);
    }

    public boolean retain(int id) {
        if (maxId < id) {
            maxId = id;
            return true;
        }
        return false;
    }

    public int getId() {
        return id;
    }

    public int getMaxId() {
        return maxId;
    }

    public long sizeInBytes() {
        return 8;
    }

    @Override
    public String toString() {
        return "MiruFieldIndexKey{" + "id=" + id + ", maxId=" + maxId + '}';
    }
}
