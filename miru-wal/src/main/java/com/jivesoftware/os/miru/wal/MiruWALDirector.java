package com.jivesoftware.os.miru.wal;

import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.topology.MiruIngressUpdate;
import com.jivesoftware.os.miru.api.topology.MiruPartitionStatus;
import com.jivesoftware.os.miru.api.topology.RangeMinMax;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.lookup.MiruWALLookup;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class MiruWALDirector {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruWALLookup walLookup;
    private final MiruWALClient<?, ?> walClient;
    private final MiruActivityWALReader<?, ?> activityWALReader;
    private final MiruActivityWALWriter activityWALWriter;
    private final MiruClusterClient clusterClient;

    public MiruWALDirector(MiruWALLookup walLookup,
        MiruWALClient<?, ?> walClient,
        MiruActivityWALReader<?, ?> activityWALReader,
        MiruActivityWALWriter activityWALWriter,
        MiruClusterClient clusterClient) {
        this.walLookup = walLookup;
        this.walClient = walClient;
        this.activityWALReader = activityWALReader;
        this.activityWALWriter = activityWALWriter;
        this.clusterClient = clusterClient;
    }

    public void repairRanges(boolean fast) throws Exception {
        List<MiruTenantId> tenantIds = walClient.getAllTenantIds();
        for (MiruTenantId tenantId : tenantIds) {
            final Set<MiruPartitionId> found = Sets.newHashSet();
            final Set<MiruPartitionId> broken = Sets.newHashSet();
            /*TODO ask clusterClient for partitionIds with broken/missing ranges
            walLookup.streamRanges(tenantId, null, (partitionId, type, timestamp) -> {
                found.add(partitionId);
                if (type == MiruWALLookup.RangeType.orderIdMax && timestamp == Long.MAX_VALUE) {
                    broken.add(partitionId);
                }
                return true;
            });
            */

            int count = 0;
            MiruPartitionId latestPartitionId = walClient.getLargestPartitionId(tenantId);
            for (MiruPartitionId checkPartitionId = latestPartitionId; checkPartitionId != null; checkPartitionId = checkPartitionId.prev()) {
                if (!found.contains(checkPartitionId) || broken.contains(checkPartitionId)) {
                    count++;
                    if (fast) {
                        long clockMax = activityWALReader.clockMax(tenantId, checkPartitionId);
                        if (clockMax != -1) {
                            RangeMinMax minMax = new RangeMinMax();
                            minMax.clockMax = clockMax;
                            clusterClient.updateIngress(new MiruIngressUpdate(tenantId, checkPartitionId, minMax, -1, false));
                        }
                    } else {
                        RangeMinMax minMax = new RangeMinMax();
                        activityWALReader.stream(tenantId, checkPartitionId, null, 1_000, -1L,
                            (collisionId, partitionedActivity, timestamp) -> {
                                if (partitionedActivity.type.isActivityType()) {
                                    minMax.put(partitionedActivity.clockTimestamp, partitionedActivity.timestamp);
                                }
                                return true;
                            });
                        clusterClient.updateIngress(new MiruIngressUpdate(tenantId, checkPartitionId, minMax, -1, true));
                    }
                }
            }

            LOG.info("Repaired ranges in {} partitions for tenant {}", count, tenantId);
        }
    }

    public void removeCleanup() throws Exception {
        LOG.info("Beginning scan for partitions to clean up");
        List<MiruTenantId> tenantIds = walClient.getAllTenantIds();
        for (MiruTenantId tenantId : tenantIds) {
            List<MiruPartitionStatus> status = getAllPartitionStatus(tenantId);
            int[] count = { 0 };
            filterCleanup(status, false, (partitionStatus, isLatest) -> {
                removePartition(tenantId, partitionStatus.getPartitionId());
                count[0]++;
                return true;
            });
            if (count[0] > 0) {
                LOG.info("Removed {} partitions for tenant {}", count, tenantId);
            }
        }
        LOG.info("Finished scan for partitions to clean up");
    }

    public void filterCleanup(List<MiruPartitionStatus> status, boolean includeLatestPartitions, PartitionStatusStream stream) throws Exception {
        MiruPartitionId latestPartitionId = null;
        for (MiruPartitionStatus partitionStatus : status) {
            if (latestPartitionId == null || latestPartitionId.compareTo(partitionStatus.getPartitionId()) < 0) {
                latestPartitionId = partitionStatus.getPartitionId();
            }
        }

        for (MiruPartitionStatus partitionStatus : status) {
            boolean isLatest = partitionStatus.getPartitionId().equals(latestPartitionId);
            if (partitionStatus.getCleanupAfterTimestamp() > 0
                && System.currentTimeMillis() > partitionStatus.getCleanupAfterTimestamp()
                && (includeLatestPartitions || !isLatest)) {
                if (!stream.stream(partitionStatus, isLatest)) {
                    break;
                }
            }
        }
    }

    public interface PartitionStatusStream {

        boolean stream(MiruPartitionStatus partitionStatus, boolean isLatest) throws Exception;
    }

    public List<MiruPartitionStatus> getAllPartitionStatus(MiruTenantId tenantId) throws Exception {
        MiruPartitionId largestPartitionId = walClient.getLargestPartitionId(tenantId);
        return clusterClient.getPartitionStatus(tenantId, largestPartitionId);
    }

    public void removePartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        activityWALWriter.removePartition(tenantId, partitionId);
        // We do not remove ingress because it acts as tombstones
    }

    private long packTimestamp(long millisIntoTheFuture) {
        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        long epochTime = System.currentTimeMillis() + millisIntoTheFuture - JiveEpochTimestampProvider.JIVE_EPOCH;
        return snowflakeIdPacker.pack(epochTime, 0, 0);
    }

    private void repairLookup() throws Exception {
        LOG.info("Repairing lookup...");
        int[] count = new int[1];
        activityWALReader.allPartitions((tenantId, partitionId) -> {
            walLookup.add(tenantId, partitionId);
            count[0]++;
            return true;
        });
        walLookup.markRepaired();
        LOG.info("Finished repairing lookup for {} partitions", count[0]);
    }

}
