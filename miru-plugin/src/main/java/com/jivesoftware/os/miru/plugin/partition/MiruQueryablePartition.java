package com.jivesoftware.os.miru.plugin.partition;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;

/**
 *
 * @author jonathan.colt
 */
public interface MiruQueryablePartition<BM> {

    MiruPartitionCoord getCoord();

    boolean isLocal();

    MiruRequestHandle<BM, ?> acquireQueryHandle() throws Exception;

    MiruRequestHandle<BM, ?> tryQueryHandle() throws Exception;
}
