package com.jivesoftware.os.miru.plugin.partition;

import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.Iterator;

/**
 *
 * @author jonathan
 */
public interface MiruHostedPartition {

    void remove() throws Exception;

    MiruPartitionCoord getCoord();

    MiruTenantId getTenantId();

    MiruPartitionId getPartitionId();

    MiruPartitionState getState();

    MiruBackingStorage getStorage();

    void index(Iterator<MiruPartitionedActivity> activities) throws Exception;

    void warm() throws Exception;

    void compact() throws Exception;

    boolean rebuild() throws Exception;
}
