package com.jivesoftware.os.miru.wal;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class MiruWALRepair {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruWALClient<? ,?> walClient;
    private final MiruWALDirector<?, ?> walDirector;
    private final MiruActivityWALReader<?, ?> activityWALReader;

    private final MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();

    public MiruWALRepair(MiruWALClient<?, ?> walClient,
        MiruWALDirector<?, ?> walDirector,
        MiruActivityWALReader<?, ?> activityWALReader) {
        this.walClient = walClient;
        this.walDirector = walDirector;
        this.activityWALReader = activityWALReader;
    }

    public void repairBoundaries() throws Exception {
        List<MiruTenantId> tenantIds = walDirector.getAllTenantIds();
        for (MiruTenantId tenantId : tenantIds) {
            MiruPartitionId latestPartitionId = walDirector.getLargestPartitionId(tenantId);
            if (latestPartitionId != null) {
                for (MiruPartitionId partitionId = latestPartitionId.prev(); partitionId != null; partitionId = partitionId.prev()) {
                    MiruActivityWALStatus status = activityWALReader.getStatus(tenantId, partitionId);
                    if (!status.begins.equals(status.ends)) {
                        for (int begin : status.begins) {
                            if (!status.ends.contains(begin)) {
                                walClient.writeActivity(tenantId, partitionId,
                                    Arrays.asList(partitionedActivityFactory.end(begin, partitionId, tenantId, -1)));
                                LOG.info("Added missing 'end' to WAL for tenant:{} partition:{} writer:{}", tenantId, partitionId, begin);
                            }
                        }
                        for (int end : status.ends) {
                            if (!status.begins.contains(end)) {
                                walClient.writeActivity(tenantId, partitionId,
                                    Arrays.asList(partitionedActivityFactory.begin(end, partitionId, tenantId, -1)));
                                LOG.info("Added missing 'begin' to WAL for tenant:{} partition:{} writer:{}", tenantId, partitionId, end);
                            }
                        }
                    }
                }
            }
        }
    }
}
