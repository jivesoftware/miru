package com.jivesoftware.os.miru.manage.deployable;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALStatus;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.lookup.MiruActivityLookupTable;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MiruWALDirector {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruActivityLookupTable activityLookupTable;
    private final MiruActivityWALReader activityWALReader;
    private final MiruActivityWALWriter activityWALWriter;

    private final MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();

    public MiruWALDirector(MiruActivityLookupTable activityLookupTable,
        MiruActivityWALReader activityWALReader,
        MiruActivityWALWriter activityWALWriter) {
        this.activityLookupTable = activityLookupTable;
        this.activityWALReader = activityWALReader;
        this.activityWALWriter = activityWALWriter;
    }

    public void repairActivityWAL() throws Exception {
        List<MiruTenantId> tenantIds = activityLookupTable.allTenantIds();
        for (MiruTenantId tenantId : tenantIds) {
            Optional<MiruPartitionId> latestPartitionId = activityWALReader.getLatestPartitionIdForTenant(tenantId);
            if (latestPartitionId.isPresent()) {
                for (MiruPartitionId partitionId = latestPartitionId.get().prev(); partitionId != null; partitionId = partitionId.prev()) {
                    MiruActivityWALStatus status = activityWALReader.getStatus(tenantId, partitionId);
                    if (!status.begins.equals(status.ends)) {
                        for (int begin : status.begins) {
                            if (!status.ends.contains(begin)) {
                                activityWALWriter.write(tenantId, Arrays.asList(partitionedActivityFactory.end(begin, partitionId, tenantId, -1)));
                                LOG.info("Added missing 'end' to WAL for {} {}", tenantId, partitionId);
                            }
                        }
                        for (int end : status.ends) {
                            if (!status.begins.contains(end)) {
                                activityWALWriter.write(tenantId, Arrays.asList(partitionedActivityFactory.begin(end, partitionId, tenantId, -1)));
                                LOG.info("Added missing 'begin' to WAL for {} {}", tenantId, partitionId);
                            }
                        }
                    }
                }
            }
        }
    }

    private long packTimestamp(long millisIntoTheFuture) {
        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        long epochTime = System.currentTimeMillis() + millisIntoTheFuture - JiveEpochTimestampProvider.JIVE_EPOCH;
        return snowflakeIdPacker.pack(epochTime, 0, 0);
    }

    public void sanitizeActivityWAL(final MiruTenantId tenantId, final MiruPartitionId partitionId) throws Exception {
        long afterTimestamp = packTimestamp(TimeUnit.DAYS.toMillis(1));
        final List<MiruActivityWALColumnKey> badKeys = Lists.newArrayList();
        activityWALReader.stream(tenantId, partitionId, afterTimestamp, 10_000, 1_000, new MiruActivityWALReader.StreamMiruActivityWAL() {
            @Override
            public boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception {
                if (partitionedActivity.type.isActivityType()) {
                    LOG.warn("Sanitizer is removing activity " + collisionId + "/" + partitionedActivity.timestamp + " from " +
                        tenantId + "/" + partitionId);
                    badKeys.add(new MiruActivityWALColumnKey(partitionedActivity.type.getSort(), collisionId));
                }
                return partitionedActivity.type.isActivityType();
            }
        });
        activityWALReader.delete(tenantId, partitionId, badKeys);
    }

    public void sanitizeActivitySipWAL(final MiruTenantId tenantId, final MiruPartitionId partitionId) throws Exception {
        final long afterTimestamp = packTimestamp(TimeUnit.DAYS.toMillis(1));
        final List<MiruActivitySipWALColumnKey> badKeys = Lists.newArrayList();
        activityWALReader.streamSip(tenantId, partitionId, new MiruActivityWALReader.Sip(0, 0), 10_000, 1_000,
            new MiruActivityWALReader.StreamMiruActivityWAL() {
                @Override
                public boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception {
                    if (partitionedActivity.timestamp > afterTimestamp && partitionedActivity.type.isActivityType()) {
                        LOG.warn("Sanitizer is removing sip activity " + collisionId + "/" + partitionedActivity.timestamp + " from " +
                            tenantId + "/" + partitionId);
                        badKeys.add(new MiruActivitySipWALColumnKey(partitionedActivity.type.getSort(), collisionId, timestamp));
                    }
                    return partitionedActivity.type.isActivityType();
                }
            });
        activityWALReader.deleteSip(tenantId, partitionId, badKeys);
    }

}
