package com.jivesoftware.os.miru.plugin.partition;

import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;

/**
 * @author jonathan.colt
 */
public interface MiruQueryablePartition<BM extends IBM, IBM> {

    MiruPartitionCoord getCoord();

    MiruPartitionState getState();

    MiruBackingStorage getStorage();

    boolean isLocal();

    MiruRequestHandle<BM, IBM, ?> acquireQueryHandle() throws Exception;

    MiruRequestHandle<BM, IBM, ?> inspectRequestHandle(boolean hotDeploy) throws Exception;

    boolean isAvailable();
}
