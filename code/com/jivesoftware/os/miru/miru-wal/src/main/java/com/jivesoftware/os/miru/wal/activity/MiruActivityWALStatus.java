package com.jivesoftware.os.miru.wal.activity;

import java.util.List;

/**
 *
 */
public class MiruActivityWALStatus {

    public final long count;
    public final List<Integer> begins;
    public final List<Integer> ends;

    public MiruActivityWALStatus(long count, List<Integer> begins, List<Integer> ends) {
        this.count = count;
        this.begins = begins;
        this.ends = ends;
    }
}
