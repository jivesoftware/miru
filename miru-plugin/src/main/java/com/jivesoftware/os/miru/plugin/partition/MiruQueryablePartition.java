package com.jivesoftware.os.miru.plugin.partition;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;

/**
 * @author jonathan.colt
 */
public interface MiruQueryablePartition<BM extends IBM, IBM> {

    MiruPartitionCoord getCoord();

    boolean isLocal();

    MiruRequestHandle<BM, IBM, ?> acquireQueryHandle(byte[] primitiveBuffer) throws Exception;

    MiruRequestHandle<BM, IBM, ?> inspectRequestHandle() throws Exception;
}
