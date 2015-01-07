package com.jivesoftware.os.miru.plugin.partition;

import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import java.util.Iterator;

/**
 *
 * @author jonathan
 */
public interface MiruHostedPartition<BM> {

    void remove() throws Exception;

    boolean isLocal();

    MiruPartitionCoord getCoord();

    MiruTenantId getTenantId();

    MiruPartitionId getPartitionId();

    MiruPartitionState getState();

    MiruBackingStorage getStorage();

    void index(Iterator<MiruPartitionedActivity> activities) throws Exception;

    void warm();

    void setStorage(MiruBackingStorage storage) throws Exception;

    MiruRequestHandle<BM> getQueryHandle() throws Exception;
}
