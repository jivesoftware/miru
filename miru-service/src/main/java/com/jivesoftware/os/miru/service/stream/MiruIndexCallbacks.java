package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.plugin.plugin.IndexCloseCallback;
import com.jivesoftware.os.miru.plugin.plugin.IndexCommitCallback;
import com.jivesoftware.os.miru.plugin.plugin.IndexOpenCallback;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 *
 */
public class MiruIndexCallbacks {

    public final List<IndexOpenCallback> openCallbacks = new CopyOnWriteArrayList<>();
    public final List<IndexCommitCallback> commitCallbacks = new CopyOnWriteArrayList<>();
    public final List<IndexCloseCallback> closeCallbacks = new CopyOnWriteArrayList<>();

    public void open(MiruPartitionCoord coord) {
        for (IndexOpenCallback callback : openCallbacks) {
            callback.indexOpen(coord);
        }
    }

    public void commit(MiruPartitionCoord coord) {
        for (IndexCommitCallback callback : commitCallbacks) {
            callback.indexCommit(coord);
        }
    }

    public boolean canClose(MiruPartitionCoord coord) {
        for (IndexCloseCallback callback : closeCallbacks) {
            if (!callback.canClose(coord)) {
                return false;
            }
        }
        return true;
    }

    public void close(MiruPartitionCoord coord) {
        for (IndexCloseCallback callback : closeCallbacks) {
            callback.indexClose(coord);
        }
    }
}
