package com.jivesoftware.os.miru.wal.activity;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.RangeMinMax;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class ForkingActivityWALWriter implements MiruActivityWALWriter {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruActivityWALWriter primaryWAL;
    private final MiruActivityWALWriter secondaryWAL;

    public ForkingActivityWALWriter(MiruActivityWALWriter primaryWAL, MiruActivityWALWriter secondaryWAL) {
        this.primaryWAL = primaryWAL;
        this.secondaryWAL = secondaryWAL;
    }

    @Override
    public RangeMinMax write(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        LOG.startTimer("forking>write>primary");
        RangeMinMax partitionMinMax;
        try {
            partitionMinMax = primaryWAL.write(tenantId, partitionId, partitionedActivities);
        } finally {
            LOG.startTimer("forking>write>primary");
        }

        if (secondaryWAL != null) {
            LOG.startTimer("forking>write>secondary");
            try {
                while (true) {
                    try {
                        secondaryWAL.write(tenantId, partitionId, partitionedActivities);
                        break;
                    } catch (Exception x) {
                        LOG.warn("Failed to write:{} activities for tenant:{} to secondary WAL.", new Object[] { partitionedActivities.size(), tenantId }, x);
                        Thread.sleep(10_000);
                    }
                }
            } finally {
                LOG.stopTimer("forking>write>secondary");
            }
        }
        return partitionMinMax;
    }

    @Override
    public void removePartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        LOG.startTimer("forking>removePartition>primary");
        try {
            primaryWAL.removePartition(tenantId, partitionId);
        } finally {
            LOG.startTimer("forking>removePartition>primary");
        }

        if (secondaryWAL != null) {
            LOG.startTimer("forking>removePartition>secondary");
            try {
                secondaryWAL.removePartition(tenantId, partitionId);
                //TODO spin?
            } finally {
                LOG.stopTimer("forking>removePartition>secondary");
            }
        }
    }
}
