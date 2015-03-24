package com.jivesoftware.os.miru.wal.activity;

import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class ForkingActvityWALWriter implements MiruActivityWALWriter {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruActivityWALWriter primaryWAL;
    private final MiruActivityWALWriter secondaryWAL;

    public ForkingActvityWALWriter(MiruActivityWALWriter primaryWAL, MiruActivityWALWriter secondaryWAL) {
        this.primaryWAL = primaryWAL;
        this.secondaryWAL = secondaryWAL;
    }

    @Override
    public void write(MiruTenantId tenantId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        if (secondaryWAL != null) {
            LOG.startTimer("forking>secondary");
            try {
                while (true) {
                    try {
                        secondaryWAL.write(tenantId, partitionedActivities);
                        break;
                    } catch (Exception x) {
                        LOG.warn("Failed to write:{} activities for tenant:{} to secondary WAL.", new Object[]{partitionedActivities.size(), tenantId}, x);
                        Thread.sleep(10_000);
                    }
                }
            } finally {
                LOG.stopTimer("forking>secondary");
            }

            LOG.startTimer("forking>primary");
            try {
                primaryWAL.write(tenantId, partitionedActivities);
            } finally {
                LOG.startTimer("forking>primary");
            }
        }
    }

}
