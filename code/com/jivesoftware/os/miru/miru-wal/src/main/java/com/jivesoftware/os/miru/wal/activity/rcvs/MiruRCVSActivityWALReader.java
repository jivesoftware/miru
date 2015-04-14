package com.jivesoftware.os.miru.wal.activity.rcvs;

import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.Sip;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.partition.MiruPartitionCursor;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.TenantIdAndRow;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.mutable.MutableLong;

/** @author jonathan */
public class MiruRCVSActivityWALReader implements MiruActivityWALReader {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL;
    private final RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception>
        activitySipWAL;

    public MiruRCVSActivityWALReader(
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL,
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> activitySipWAL
    ) {

        this.activityWAL = activityWAL;
        this.activitySipWAL = activitySipWAL;
    }

    private MiruActivityWALRow rowKey(MiruPartitionId partition) {
        return new MiruActivityWALRow(partition.getId());
    }

    @Override
    public void stream(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        byte sortOrder,
        long afterTimestamp,
        final int batchSize,
        StreamMiruActivityWAL streamMiruActivityWAL)
        throws Exception {

        MiruActivityWALRow rowKey = rowKey(partitionId);

        final List<ColumnValueAndTimestamp<MiruActivityWALColumnKey, MiruPartitionedActivity, Long>> cvats = Lists.newArrayListWithCapacity(batchSize);
        boolean streaming = true;
        byte lastSort = sortOrder;
        long lastTimestamp = afterTimestamp;
        while (streaming) {
            MiruActivityWALColumnKey start = new MiruActivityWALColumnKey(lastSort, lastTimestamp);
            activityWAL.getEntrys(tenantId, rowKey, start, Long.MAX_VALUE, batchSize, false, null, null,
                new CallbackStream<ColumnValueAndTimestamp<MiruActivityWALColumnKey, MiruPartitionedActivity, Long>>() {
                    @Override
                    public ColumnValueAndTimestamp<MiruActivityWALColumnKey, MiruPartitionedActivity, Long> callback(
                        ColumnValueAndTimestamp<MiruActivityWALColumnKey, MiruPartitionedActivity, Long> v) throws Exception {

                        if (v != null) {
                            cvats.add(v);
                        }
                        if (cvats.size() < batchSize) {
                            return v;
                        } else {
                            return null;
                        }
                    }
                });

            if (cvats.size() < batchSize) {
                streaming = false;
            }
            for (ColumnValueAndTimestamp<MiruActivityWALColumnKey, MiruPartitionedActivity, Long> v : cvats) {
                if (streamMiruActivityWAL.stream(v.getColumn().getCollisionId(), v.getValue(), v.getTimestamp())) {
                    // add 1 to exclude last result
                    lastSort = v.getColumn().getSort();
                    lastTimestamp = v.getColumn().getCollisionId() + 1;
                } else {
                    streaming = false;
                    break;
                }
            }
            cvats.clear();
        }
    }

    @Override
    public void streamSip(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        byte sortOrder,
        Sip afterSip,
        final int batchSize,
        final StreamMiruActivityWAL streamMiruActivityWAL)
        throws Exception {

        MiruActivityWALRow rowKey = rowKey(partitionId);

        final List<ColumnValueAndTimestamp<MiruActivitySipWALColumnKey, MiruPartitionedActivity, Long>> cvats = Lists.newArrayListWithCapacity(batchSize);
        boolean streaming = true;
        byte lastSort = sortOrder;
        long lastClockTimestamp = afterSip.clockTimestamp;
        long lastActivityTimestamp = afterSip.activityTimestamp;
        while (streaming) {
            MiruActivitySipWALColumnKey start = new MiruActivitySipWALColumnKey(lastSort, lastClockTimestamp, lastActivityTimestamp);
            activitySipWAL.getEntrys(tenantId, rowKey, start, Long.MAX_VALUE, batchSize, false, null, null,
                new CallbackStream<ColumnValueAndTimestamp<MiruActivitySipWALColumnKey, MiruPartitionedActivity, Long>>() {
                    @Override
                    public ColumnValueAndTimestamp<MiruActivitySipWALColumnKey, MiruPartitionedActivity, Long> callback(
                        ColumnValueAndTimestamp<MiruActivitySipWALColumnKey, MiruPartitionedActivity, Long> v) throws Exception {

                        if (v != null) {
                            cvats.add(v);
                        }
                        if (cvats.size() < batchSize) {
                            return v;
                        } else {
                            return null;
                        }
                    }
                });

            if (cvats.size() < batchSize) {
                streaming = false;
            }
            for (ColumnValueAndTimestamp<MiruActivitySipWALColumnKey, MiruPartitionedActivity, Long> v : cvats) {
                if (streamMiruActivityWAL.stream(v.getColumn().getCollisionId(), v.getValue(), v.getTimestamp())) {
                    // add 1 to exclude last result
                    lastSort = v.getColumn().getSort();
                    lastClockTimestamp = v.getColumn().getCollisionId();
                    lastActivityTimestamp = v.getColumn().getSipId() + 1;
                } else {
                    streaming = false;
                    break;
                }
            }
            cvats.clear();
        }
    }

    @Override
    public MiruActivityWALStatus getStatus(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        final MutableLong count = new MutableLong(0);
        final List<Integer> begins = Lists.newArrayList();
        final List<Integer> ends = Lists.newArrayList();
        activityWAL.getValues(tenantId,
            new MiruActivityWALRow(partitionId.getId()),
            new MiruActivityWALColumnKey(MiruPartitionedActivity.Type.END.getSort(), 0),
            null, 1_000, false, null, null, new CallbackStream<MiruPartitionedActivity>() {
                @Override
                public MiruPartitionedActivity callback(MiruPartitionedActivity partitionedActivity) throws Exception {
                    if (partitionedActivity != null) {
                        if (partitionedActivity.type == MiruPartitionedActivity.Type.BEGIN) {
                            count.add(partitionedActivity.index);
                            begins.add(partitionedActivity.writerId);
                        } else if (partitionedActivity.type == MiruPartitionedActivity.Type.END) {
                            ends.add(partitionedActivity.writerId);
                        }
                    }
                    return partitionedActivity;
                }
            });
        return new MiruActivityWALStatus(partitionId, count.longValue(), begins, ends);
    }

    @Override
    public long countSip(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        final MutableLong count = new MutableLong(0);
        activitySipWAL.getValues(tenantId,
            new MiruActivityWALRow(partitionId.getId()),
            new MiruActivitySipWALColumnKey(MiruPartitionedActivity.Type.BEGIN.getSort(), 0, 0),
            null, 1_000, false, null, null, new CallbackStream<MiruPartitionedActivity>() {
                @Override
                public MiruPartitionedActivity callback(MiruPartitionedActivity partitionedActivity) throws Exception {
                    if (partitionedActivity != null && partitionedActivity.type == MiruPartitionedActivity.Type.BEGIN) {
                        count.add(partitionedActivity.index);
                    }
                    return partitionedActivity;
                }
            });
        return count.longValue();
    }

    @Override
    public MiruPartitionedActivity findExisting(MiruTenantId tenantId, MiruPartitionId partitionId, MiruPartitionedActivity activity) throws Exception {
        return activityWAL.get(
            tenantId,
            new MiruActivityWALRow(partitionId.getId()),
            new MiruActivityWALColumnKey(MiruPartitionedActivity.Type.ACTIVITY.getSort(), activity.timestamp),
            null, null);
    }

    @Override
    public long oldestActivityClockTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        final MutableLong oldestClockTimestamp = new MutableLong(-1);
        activityWAL.getValues(tenantId,
            new MiruActivityWALRow(partitionId.getId()),
            new MiruActivityWALColumnKey(MiruPartitionedActivity.Type.ACTIVITY.getSort(), 0L),
            null,
            1,
            false,
            null,
            null,
            new CallbackStream<MiruPartitionedActivity>() {
                @Override
                public MiruPartitionedActivity callback(MiruPartitionedActivity miruPartitionedActivity) throws Exception {
                    if (miruPartitionedActivity != null && miruPartitionedActivity.type.isActivityType()) {
                        oldestClockTimestamp.setValue(miruPartitionedActivity.clockTimestamp);
                    }
                    return null; // one and done
                }
            });
        return oldestClockTimestamp.longValue();
    }

    @Override
    public void delete(MiruTenantId tenantId, MiruPartitionId partitionId, Collection<MiruActivityWALColumnKey> keys) throws Exception {
        MiruActivityWALColumnKey[] remove = keys.toArray(new MiruActivityWALColumnKey[keys.size()]);
        activityWAL.multiRemove(tenantId, new MiruActivityWALRow(partitionId.getId()), remove, null);
    }

    @Override
    public void deleteSip(MiruTenantId tenantId, MiruPartitionId partitionId, Collection<MiruActivitySipWALColumnKey> keys) throws Exception {
        MiruActivitySipWALColumnKey[] remove = keys.toArray(new MiruActivitySipWALColumnKey[keys.size()]);
        activitySipWAL.multiRemove(tenantId, new MiruActivityWALRow(partitionId.getId()), remove, null);
    }

    @Override
    public MiruPartitionCursor getCursorForWriterId(MiruTenantId tenantId, final int writerId, int desiredPartitionCapacity) throws Exception {
        LOG.inc("getCursorForWriterId");
        LOG.inc("getCursorForWriterId>" + writerId, tenantId.toString());
        int partitionId = 0;
        int largestRunOfGaps = 10; // TODO expose to config?
        MiruPartitionedActivity latestBoundaryActivity = null;
        int gaps = 0;
        while (true) {
            MiruPartitionedActivity got = activityWAL.get(tenantId,
                new MiruActivityWALRow(partitionId),
                new MiruActivityWALColumnKey(MiruPartitionedActivity.Type.BEGIN.getSort(), (long) writerId),
                null, null);
            if (got != null) {
                gaps = 0;
                latestBoundaryActivity = got;
            } else {
                gaps++;
                if (gaps > largestRunOfGaps) {
                    break;
                }
            }
            partitionId++;
        }

        if (latestBoundaryActivity == null) {
            return new MiruPartitionCursor(MiruPartitionId.of(0),
                new AtomicInteger(0),
                desiredPartitionCapacity);
        } else {
            return new MiruPartitionCursor(MiruPartitionId.of(latestBoundaryActivity.getPartitionId()),
                new AtomicInteger(latestBoundaryActivity.index),
                desiredPartitionCapacity);
        }
    }

    @Override
    public MiruPartitionCursor getPartitionCursorForWriterId(MiruTenantId tenantId, MiruPartitionId partitionId, int writerId, int desiredPartitionCapacity)
        throws Exception {
        LOG.inc("getPartitionCursorForWriterId");
        LOG.inc("getPartitionCursorForWriterId>" + partitionId + '>' + writerId, tenantId.toString());
        MiruPartitionedActivity got = activityWAL.get(tenantId,
            new MiruActivityWALRow(partitionId.getId()),
            new MiruActivityWALColumnKey(MiruPartitionedActivity.Type.BEGIN.getSort(), (long) writerId),
            null, null);
        int index = (got != null) ? got.index : 0;
        return new MiruPartitionCursor(partitionId,
            new AtomicInteger(index),
            desiredPartitionCapacity);
    }

    @Override
    public void allPartitions(final PartitionsStream stream) throws Exception {
        activityWAL.getAllRowKeys(10_000, null, new CallbackStream<TenantIdAndRow<MiruTenantId, MiruActivityWALRow>>() {
            @Override
            public TenantIdAndRow<MiruTenantId, MiruActivityWALRow> callback(TenantIdAndRow<MiruTenantId, MiruActivityWALRow>
                tenantIdAndRow) throws Exception {
                if (tenantIdAndRow != null) {
                    if (!stream.stream(tenantIdAndRow.getTenantId(), MiruPartitionId.of(tenantIdAndRow.getRow().getPartitionId()))) {
                        return null;
                    }
                }
                return tenantIdAndRow;
            }
        });
    }
}
