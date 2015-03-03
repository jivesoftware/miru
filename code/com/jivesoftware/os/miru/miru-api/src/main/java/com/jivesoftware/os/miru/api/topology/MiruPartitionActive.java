package com.jivesoftware.os.miru.api.topology;

/**
 *
 */
public class MiruPartitionActive {

    public final boolean active;
    public final boolean idle;

    public MiruPartitionActive(boolean active, boolean idle) {
        this.active = active;
        this.idle = idle;
    }
}
