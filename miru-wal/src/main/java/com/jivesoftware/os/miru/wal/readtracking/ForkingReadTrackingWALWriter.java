package com.jivesoftware.os.miru.wal.readtracking;

import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class ForkingReadTrackingWALWriter implements MiruReadTrackingWALWriter {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruReadTrackingWALWriter primaryWAL;
    private final MiruReadTrackingWALWriter secondaryWAL;

    public ForkingReadTrackingWALWriter(MiruReadTrackingWALWriter primaryWAL, MiruReadTrackingWALWriter secondaryWAL) {
        this.primaryWAL = primaryWAL;
        this.secondaryWAL = secondaryWAL;
    }

    @Override
    public void write(MiruTenantId tenantId, MiruStreamId streamId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        LOG.startTimer("forking>write>primary");
        try {
            primaryWAL.write(tenantId, streamId, partitionedActivities);
        } finally {
            LOG.startTimer("forking>write>primary");
        }

        if (secondaryWAL != null) {
            LOG.startTimer("forking>write>secondary");
            try {
                while (true) {
                    try {
                        secondaryWAL.write(tenantId, streamId, partitionedActivities);
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
}
