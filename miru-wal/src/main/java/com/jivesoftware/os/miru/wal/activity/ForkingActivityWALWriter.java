package com.jivesoftware.os.miru.wal.activity;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
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
    public void write(MiruTenantId tenantId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        LOG.startTimer("forking>write>primary");
        try {
            primaryWAL.write(tenantId, partitionedActivities);
        } finally {
            LOG.startTimer("forking>write>primary");
        }

        if (secondaryWAL != null) {
            LOG.startTimer("forking>write>secondary");
            try {
                while (true) {
                    try {
                        secondaryWAL.write(tenantId, partitionedActivities);
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
    }

    @Override
    public void copyPartition(MiruTenantId tenantId, MiruPartitionId from, MiruPartitionId to, int batchSize) throws Exception {
        LOG.startTimer("forking>copyPartition>primary");
        try {
            primaryWAL.copyPartition(tenantId, from, to, batchSize);
        } finally {
            LOG.startTimer("forking>copyPartition>primary");
        }

        if (secondaryWAL != null) {
            LOG.startTimer("forking>copyPartition>secondary");
            try {
                secondaryWAL.copyPartition(tenantId, from, to, batchSize);
                //TODO spin?
            } finally {
                LOG.stopTimer("forking>copyPartition>secondary");
            }
        }
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
