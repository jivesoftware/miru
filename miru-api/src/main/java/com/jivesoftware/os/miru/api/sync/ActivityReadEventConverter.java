package com.jivesoftware.os.miru.api.sync;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 *
 */
public interface ActivityReadEventConverter {

    Map<MiruStreamId, List<MiruPartitionedActivity>> convert(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        List<MiruPartitionedActivity> activities) throws Exception;
}
