package com.jivesoftware.os.miru.plugin.plugin;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;

/**
 *
 */
public interface IndexCloseCallback {

    boolean canClose(MiruPartitionCoord coord);

    void indexClose(MiruPartitionCoord coord);
}
