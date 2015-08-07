package com.jivesoftware.os.miru.wal.activity.rcvs;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.HostPortProvider;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.WriterCursor;
import com.jivesoftware.os.miru.api.wal.RCVSCursor;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import java.util.List;
import org.apache.commons.lang.mutable.MutableLong;

/** @author jonathan */
public class RCVSActivityWALReader implements MiruActivityWALReader<RCVSCursor, RCVSSipCursor> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final HostPortProvider hostPortProvider;
    private final RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL;
    private final RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception>
        activitySipWAL;

    public RCVSActivityWALReader(
        HostPortProvider hostPortProvider,
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL,
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> activitySipWAL) {
        this.hostPortProvider = hostPortProvider;
        this.activityWAL = activityWAL;
        this.activitySipWAL = activitySipWAL;
    }

    private MiruActivityWALRow rowKey(MiruPartitionId partition) {
        return new MiruActivityWALRow(partition.getId());
    }

    @Override
    public HostPort[] getRoutingGroup(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        RowColumnValueStore.HostAndPort hostAndPort = activityWAL.locate(tenantId, rowKey(partitionId));
        int port = hostPortProvider.getPort(hostAndPort.host);
        if (port < 0) {
            return new HostPort[0];
        } else {
            return new HostPort[] { new HostPort(hostAndPort.host, port) };
        }
    }

    @Override
    public RCVSCursor stream(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        RCVSCursor afterCursor,
        final int batchSize,
        StreamMiruActivityWAL streamMiruActivityWAL)
        throws Exception {

        if (afterCursor != null && afterCursor.endOfStream) {
            return afterCursor;
        }

        MiruActivityWALRow rowKey = rowKey(partitionId);

        final List<ColumnValueAndTimestamp<MiruActivityWALColumnKey, MiruPartitionedActivity, Long>> cvats = Lists.newArrayListWithCapacity(batchSize);
        boolean streaming = true;
        boolean endOfStream = false;
        if (afterCursor == null) {
            afterCursor = RCVSCursor.INITIAL;
        }
        byte nextSort = afterCursor.sort;
        long nextTimestamp = afterCursor.activityTimestamp;
        MiruPartitionedActivity lastActivity = null;
        while (streaming) {
            MiruActivityWALColumnKey start = new MiruActivityWALColumnKey(nextSort, nextTimestamp);
            activityWAL.getEntrys(tenantId, rowKey, start, Long.MAX_VALUE, batchSize, false, null, null,
                (ColumnValueAndTimestamp<MiruActivityWALColumnKey, MiruPartitionedActivity, Long> v) -> {
                    if (v != null) {
                        cvats.add(v);
                    }
                    if (cvats.size() < batchSize) {
                        return v;
                    } else {
                        return null;
                    }
                });

            if (cvats.size() < batchSize) {
                endOfStream = true;
                streaming = false;
            }
            for (ColumnValueAndTimestamp<MiruActivityWALColumnKey, MiruPartitionedActivity, Long> v : cvats) {
                byte sort = v.getColumn().getSort();
                long collisionId = v.getColumn().getCollisionId();
                MiruPartitionedActivity partitionedActivity = v.getValue();
                if (streamMiruActivityWAL.stream(collisionId, partitionedActivity, v.getTimestamp())) {
                    if (partitionedActivity.type.isActivityType()) {
                        lastActivity = partitionedActivity;
                    }
                    // add 1 to exclude last result
                    if (collisionId == Long.MAX_VALUE) {
                        if (sort == Byte.MAX_VALUE) {
                            nextSort = sort;
                            nextTimestamp = collisionId;
                            endOfStream = true;
                            streaming = false;
                        } else {
                            nextSort = (byte) (sort + 1);
                            nextTimestamp = Long.MIN_VALUE;
                        }
                    } else {
                        nextSort = sort;
                        nextTimestamp = collisionId + 1;
                    }
                } else {
                    streaming = false;
                    nextSort = sort;
                    nextTimestamp = collisionId;
                    break;
                }
            }
            cvats.clear();
        }

        RCVSSipCursor sipCursor = null;
        if (lastActivity != null) {
            sipCursor = new RCVSSipCursor(lastActivity.type.getSort(), lastActivity.clockTimestamp, lastActivity.timestamp, false);
        }

        return new RCVSCursor(nextSort, nextTimestamp, endOfStream, sipCursor);
    }

    @Override
    public RCVSSipCursor streamSip(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        RCVSSipCursor afterCursor,
        final int batchSize,
        final StreamMiruActivityWAL streamMiruActivityWAL)
        throws Exception {

        if (afterCursor != null && afterCursor.endOfStream) {
            return afterCursor;
        }

        MiruActivityWALRow rowKey = rowKey(partitionId);

        final List<ColumnValueAndTimestamp<MiruActivitySipWALColumnKey, MiruPartitionedActivity, Long>> cvats = Lists.newArrayListWithCapacity(batchSize);
        boolean streaming = true;
        if (afterCursor == null) {
            afterCursor = RCVSSipCursor.INITIAL;
        }
        byte nextSort = afterCursor.sort;
        long nextClockTimestamp = afterCursor.clockTimestamp;
        long nextActivityTimestamp = afterCursor.activityTimestamp;
        boolean endOfStream = false;
        while (streaming) {
            MiruActivitySipWALColumnKey start = new MiruActivitySipWALColumnKey(nextSort, nextClockTimestamp, nextActivityTimestamp);
            activitySipWAL.getEntrys(tenantId, rowKey, start, Long.MAX_VALUE, batchSize, false, null, null,
                (ColumnValueAndTimestamp<MiruActivitySipWALColumnKey, MiruPartitionedActivity, Long> v) -> {
                    if (v != null) {
                        cvats.add(v);
                    }
                    if (cvats.size() < batchSize) {
                        return v;
                    } else {
                        return null;
                    }
                });

            if (cvats.size() < batchSize) {
                endOfStream = true;
                streaming = false;
            }
            for (ColumnValueAndTimestamp<MiruActivitySipWALColumnKey, MiruPartitionedActivity, Long> v : cvats) {
                byte sort = v.getColumn().getSort();
                long collisionId = v.getColumn().getCollisionId();
                long sipId = v.getColumn().getSipId();
                if (streamMiruActivityWAL.stream(collisionId, v.getValue(), v.getTimestamp())) {
                    // add 1 to exclude last result
                    if (sipId == Long.MAX_VALUE) {
                        if (collisionId == Long.MAX_VALUE) {
                            if (sort == Byte.MAX_VALUE) {
                                nextSort = sort;
                                nextClockTimestamp = collisionId;
                                nextActivityTimestamp = sipId;
                                endOfStream = true;
                                streaming = false;
                            } else {
                                nextSort = (byte) (sort + 1);
                                nextClockTimestamp = Long.MIN_VALUE;
                                nextActivityTimestamp = Long.MIN_VALUE;
                            }
                        } else {
                            nextSort = sort;
                            nextClockTimestamp = collisionId + 1;
                            nextActivityTimestamp = Long.MIN_VALUE;
                        }
                    } else {
                        nextSort = sort;
                        nextClockTimestamp = collisionId;
                        nextActivityTimestamp = sipId + 1;
                    }
                } else {
                    streaming = false;
                    nextSort = sort;
                    nextClockTimestamp = collisionId;
                    nextActivityTimestamp = sipId;
                    break;
                }
            }
            cvats.clear();
        }
        return new RCVSSipCursor(nextSort, nextClockTimestamp, nextActivityTimestamp, endOfStream);
    }

    @Override
    public MiruActivityWALStatus getStatus(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        final MutableLong count = new MutableLong(0);
        final List<Integer> begins = Lists.newArrayList();
        final List<Integer> ends = Lists.newArrayList();
        activityWAL.getValues(tenantId,
            new MiruActivityWALRow(partitionId.getId()),
            new MiruActivityWALColumnKey(MiruPartitionedActivity.Type.END.getSort(), 0),
            null, 1_000, false, null, null, partitionedActivity -> {
                if (partitionedActivity != null) {
                    if (partitionedActivity.type == MiruPartitionedActivity.Type.BEGIN) {
                        count.add(partitionedActivity.index);
                        begins.add(partitionedActivity.writerId);
                    } else if (partitionedActivity.type == MiruPartitionedActivity.Type.END) {
                        ends.add(partitionedActivity.writerId);
                    }
                }
                return partitionedActivity;
            });
        return new MiruActivityWALStatus(partitionId, count.longValue(), begins, ends);
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
            miruPartitionedActivity -> {
                if (miruPartitionedActivity != null && miruPartitionedActivity.type.isActivityType()) {
                    oldestClockTimestamp.setValue(miruPartitionedActivity.clockTimestamp);
                }
                return null; // one and done
            });
        return oldestClockTimestamp.longValue();
    }

    @Override
    public WriterCursor getCursorForWriterId(MiruTenantId tenantId, MiruPartitionId partitionId, final int writerId) throws Exception {
        LOG.inc("getCursorForWriterId");
        LOG.inc("getCursorForWriterId>" + writerId, tenantId.toString());
        MiruPartitionedActivity latestBoundaryActivity = activityWAL.get(tenantId, new MiruActivityWALRow(partitionId.getId()),
            new MiruActivityWALColumnKey(MiruPartitionedActivity.Type.BEGIN.getSort(), (long) writerId), null, null);
        if (latestBoundaryActivity != null) {
            return new WriterCursor(latestBoundaryActivity.partitionId.getId(), latestBoundaryActivity.index);
        } else {
            return new WriterCursor(0, 0);
        }
    }

    @Override
    public MiruPartitionId largestPartitionId(MiruTenantId tenantId) throws Exception {
        LOG.inc("largestPartitionId");
        long writerId = 1L; //TODO this is so bogus
        int partitionId = 0;
        int partitionsPerBatch = 10_000; //TODO config?
        MiruPartitionedActivity latestBoundaryActivity = null;
        while (true) {
            List<MiruActivityWALRow> rows = Lists.newArrayListWithCapacity(partitionsPerBatch);
            for (int i = partitionId; i < partitionId + partitionsPerBatch; i++) {
                rows.add(new MiruActivityWALRow(i));
            }
            List<MiruPartitionedActivity> batch = activityWAL.multiRowGet(tenantId, rows,
                new MiruActivityWALColumnKey(MiruPartitionedActivity.Type.BEGIN.getSort(), writerId), null, null);
            boolean found = false;
            for (MiruPartitionedActivity got : batch) {
                if (got != null) {
                    latestBoundaryActivity = got;
                    found = true;
                }
            }
            if (!found) {
                break;
            }
            partitionId += partitionsPerBatch;
        }

        if (latestBoundaryActivity == null) {
            return MiruPartitionId.of(0);
        } else {
            return latestBoundaryActivity.partitionId;
        }
    }

    @Override
    public void allPartitions(final PartitionsStream stream) throws Exception {
        activityWAL.getAllRowKeys(10_000, null, tenantIdAndRow -> {
            if (tenantIdAndRow != null) {
                if (!stream.stream(tenantIdAndRow.getTenantId(), MiruPartitionId.of(tenantIdAndRow.getRow().getPartitionId()))) {
                    return null;
                }
            }
            return tenantIdAndRow;
        });
    }
}
