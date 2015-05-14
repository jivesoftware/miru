package com.jivesoftware.os.miru.api.wal;

import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;

/**
 *
 * @author jonathan.colt
 */
public class MiruWALEntry {

    public long collisionId;
    public long version;
    public MiruPartitionedActivity activity;

    public MiruWALEntry() {
    }

    public MiruWALEntry(long collisionId, long version, MiruPartitionedActivity activity) {
        this.collisionId = collisionId;
        this.version = version;
        this.activity = activity;
    }

}
