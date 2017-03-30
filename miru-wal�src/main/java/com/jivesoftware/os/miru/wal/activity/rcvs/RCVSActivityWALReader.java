package com.jivesoftware.os.miru.wal.activity.rcvs;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.api.HostPortProvider;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity.Type;
import com.jivesoftware.os.miru.api.activity.TenantAndPartition;
import com.jivesoftware.os.miru.api.activity.TimeAndVersion;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus.WriterCount;
import com.jivesoftware.os.miru.api.wal.MiruVersionedActivityLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.WriterCursor;
import com.jivesoftware.os.miru.api.wal.RCVSCursor;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.lookup.PartitionsStream;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.mutable.MutableLong;

/** @author jonathan */
public class RCVSActivityWALReader implements MiruActivityWALReader<RCVSCursor, RCVSSipCursor> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final HostPortProvider hostPortProvider;
    private final RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL;
    private final RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception>
        activitySipWAL;
    private final Set<TenantAndPartition> blacklist;

    public RCVSActivityWALReader(
        HostPortProvider hostPortProvider,
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL,
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> activitySipWAL,
        Set<TenantAndPartition> blacklist) {
        this.hostPortProvider = hostPortProvider;
        this.activityWAL = activityWAL;
        this.activitySipWAL = activitySipWAL;
        this.blacklist = blacklist;
    }

    private MiruActivityWALRow rowKey(MiruPartitionId partition) {
        return new MiruActivityWALRow(partition.getId());
    }

    @Override
    public HostPort[] getRoutingGroup(MiruTenantId tenantId, MiruPartitionId partitionId, boolean createIfAbsent) throws Exception {
        //TODO how to blacklist?
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
        int batchSize,
        long stopAtTimestamp,
        StreamMiruActivityWAL streamMiruActivityWAL)
        throws Exception {

        if (blacklist != null && blacklist.contains(new TenantAndPartition(tenantId, partitionId))) {
            /*int writerId = -1;
            long timestamp = Long.MAX_VALUE;
            long clockTimestamp = System.currentTimeMillis();
            streamMiruActivityWAL.stream(writerId, MiruPartitionedActivity.fromJson(Type.BEGIN, writerId, partitionId.getId(), tenantId.getBytes(), 0,
                timestamp, clockTimestamp, null, null), timestamp);
            streamMiruActivityWAL.stream(writerId, MiruPartitionedActivity.fromJson(Type.END, writerId, partitionId.getId(), tenantId.getBytes(), 0,
                timestamp, clockTimestamp, null, null), timestamp);*/
            return new RCVSCursor(Byte.MAX_VALUE, Long.MAX_VALUE, true, null);
        }

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
                    if (v != null && v.getValue().type.isActivityType()) {
                        if (stopAtTimestamp > 0 && v.getValue().timestamp > stopAtTimestamp) {
                            return null;
                        }
                        cvats.add(v);
                    }
                    if (cvats.size() < batchSize) {
                        return v;
                    } else {
                        return null;
                    }
                });

            if (cvats.size() < batchSize) {
                streaming = false;
            }
            for (ColumnValueAndTimestamp<MiruActivityWALColumnKey, MiruPartitionedActivity, Long> v : cvats) {
                byte sort = v.getColumn().getSort();
                long collisionId = v.getColumn().getCollisionId();
                MiruPartitionedActivity partitionedActivity = v.getValue();
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
                if (!streamMiruActivityWAL.stream(collisionId, partitionedActivity, v.getTimestamp())) {
                    streaming = false;
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
        Set<TimeAndVersion> lastSeen,
        final int batchSize,
        final StreamMiruActivityWAL streamMiruActivityWAL,
        final StreamSuppressed streamSuppressed)
        throws Exception {

        if (blacklist != null && blacklist.contains(new TenantAndPartition(tenantId, partitionId))) {
            int writerId = -1;
            long timestamp = Long.MAX_VALUE;
            long clockTimestamp = System.currentTimeMillis();
            streamMiruActivityWAL.stream(writerId, MiruPartitionedActivity.fromJson(Type.BEGIN, writerId, partitionId.getId(), tenantId.getBytes(), 0,
                timestamp, clockTimestamp, null, null), timestamp);
            streamMiruActivityWAL.stream(writerId, MiruPartitionedActivity.fromJson(Type.END, writerId, partitionId.getId(), tenantId.getBytes(), 0,
                timestamp, clockTimestamp, null, null), timestamp);
            return new RCVSSipCursor(Byte.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, true);
        }

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
        Set<Integer> begins = Sets.newHashSet();
        Set<Integer> ends = Sets.newHashSet();
        while (streaming) {
            MiruActivitySipWALColumnKey start = new MiruActivitySipWALColumnKey(nextSort, nextClockTimestamp, nextActivityTimestamp);
            activitySipWAL.getEntrys(tenantId, rowKey, start, Long.MAX_VALUE, batchSize, false, null, null,
                (ColumnValueAndTimestamp<MiruActivitySipWALColumnKey, MiruPartitionedActivity, Long> v) -> {
                    if (v != null) {
                        MiruPartitionedActivity partitionedActivity = v.getValue();
                        if (partitionedActivity.type == MiruPartitionedActivity.Type.BEGIN) {
                            begins.add(partitionedActivity.writerId);
                        } else if (partitionedActivity.type == MiruPartitionedActivity.Type.END) {
                            ends.add(partitionedActivity.writerId);
                        } else {
                            if (!streamAlreadySeen(lastSeen, partitionedActivity, streamSuppressed)) {
                                cvats.add(v);
                            }
                        }
                    }
                    if (cvats.size() < batchSize) {
                        return v;
                    } else {
                        return null;
                    }
                });

            if (cvats.size() < batchSize) {
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
        if (!begins.isEmpty() || !ends.isEmpty()) {
            streamMiruActivityWAL.stream(-1, null, -1);
        }
        endOfStream |= !begins.isEmpty() && ends.containsAll(begins);
        return new RCVSSipCursor(nextSort, nextClockTimestamp, nextActivityTimestamp, endOfStream);
    }

    private boolean streamAlreadySeen(Set<TimeAndVersion> lastSeen,
        MiruPartitionedActivity partitionedActivity,
        StreamSuppressed streamSuppressed) throws Exception {
        if (lastSeen != null) {
            long version = partitionedActivity.activity.isPresent() ? partitionedActivity.activity.get().version : 0;
            TimeAndVersion timeAndVersion = new TimeAndVersion(partitionedActivity.timestamp, version);
            if (lastSeen.contains(timeAndVersion)) {
                if (streamSuppressed != null) {
                    streamSuppressed.stream(timeAndVersion);
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public MiruActivityWALStatus getStatus(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {

        if (blacklist != null && blacklist.contains(new TenantAndPartition(tenantId, partitionId))) {
            int writerId = -1;
            long clockTimestamp = System.currentTimeMillis();
            return new MiruActivityWALStatus(partitionId,
                Collections.singletonList(new WriterCount(writerId, 0, clockTimestamp)),
                Collections.singletonList(writerId),
                Collections.singletonList(writerId));
        }

        final Map<Integer, WriterCount> counts = Maps.newHashMap();
        final List<Integer> begins = Lists.newArrayList();
        final List<Integer> ends = Lists.newArrayList();
        activityWAL.getValues(tenantId,
            new MiruActivityWALRow(partitionId.getId()),
            new MiruActivityWALColumnKey(MiruPartitionedActivity.Type.END.getSort(), Long.MIN_VALUE),
            null, 1_000, false, null, null,
            partitionedActivity -> {
                if (partitionedActivity != null) {
                    if (partitionedActivity.type == MiruPartitionedActivity.Type.BEGIN) {
                        counts.put(partitionedActivity.writerId,
                            new WriterCount(partitionedActivity.writerId, partitionedActivity.index, partitionedActivity.clockTimestamp));
                        begins.add(partitionedActivity.writerId);
                    } else if (partitionedActivity.type == MiruPartitionedActivity.Type.END) {
                        ends.add(partitionedActivity.writerId);
                    }
                }
                return partitionedActivity;
            });
        return new MiruActivityWALStatus(partitionId, Lists.newArrayList(counts.values()), begins, ends);
    }

    @Override
    public long oldestActivityClockTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {

        if (blacklist != null && blacklist.contains(new TenantAndPartition(tenantId, partitionId))) {
            return -1;
        }

        final MutableLong oldestClockTimestamp = new MutableLong(-1);
        activityWAL.getValues(tenantId,
            new MiruActivityWALRow(partitionId.getId()),
            new MiruActivityWALColumnKey(MiruPartitionedActivity.Type.ACTIVITY.getSort(), Long.MIN_VALUE),
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

        if (blacklist != null && blacklist.contains(new TenantAndPartition(tenantId, partitionId))) {
            return new WriterCursor(0, 0);
        }

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
    public List<MiruVersionedActivityLookupEntry> getVersionedEntries(MiruTenantId tenantId, MiruPartitionId partitionId, Long[] timestamps) throws Exception {

        if (blacklist != null && blacklist.contains(new TenantAndPartition(tenantId, partitionId))) {
            return Collections.emptyList();
        }

        MiruActivityWALColumnKey[] columnKeys = new MiruActivityWALColumnKey[timestamps.length];
        int[] offsets = new int[timestamps.length];
        int index = 0;
        for (int i = 0; i < timestamps.length; i++) {
            if (timestamps[i] != null) {
                columnKeys[index] = new MiruActivityWALColumnKey(MiruPartitionedActivity.Type.ACTIVITY.getSort(), timestamps[i]);
                offsets[index] = i;
                index++;
            }
        }
        ColumnValueAndTimestamp<MiruActivityWALColumnKey, MiruPartitionedActivity, Long>[] got = activityWAL.multiGetEntries(tenantId,
            new MiruActivityWALRow(partitionId.getId()),
            columnKeys,
            null, null);

        MiruVersionedActivityLookupEntry[] entries = new MiruVersionedActivityLookupEntry[timestamps.length];
        for (int i = 0; i < got.length; i++) {
            int offset = offsets[i];
            if (got[i] == null) {
                entries[offset] = null;
            } else {
                MiruPartitionedActivity partitionedActivity = got[i].getValue();
                entries[offset] = new MiruVersionedActivityLookupEntry(timestamps[i],
                    got[i].getTimestamp(),
                    new MiruActivityLookupEntry(
                        partitionId.getId(),
                        partitionedActivity.index,
                        partitionedActivity.writerId,
                        !partitionedActivity.activity.isPresent()));
            }
        }
        return Arrays.asList(entries);
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

    @Override
    public long clockMax(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {

        if (blacklist != null && blacklist.contains(new TenantAndPartition(tenantId, partitionId))) {
            return -1;
        }

        long[] clockTimestamp = { -1L };
        activityWAL.getEntrys(tenantId, new MiruActivityWALRow(partitionId.getId()),
            new MiruActivityWALColumnKey(MiruPartitionedActivity.Type.END.getSort(), Long.MIN_VALUE), null, 10_000, false, null, null,
            value -> {
                if (value != null) {
                    clockTimestamp[0] = Math.max(clockTimestamp[0], value.getValue().clockTimestamp);
                }
                return value;
            });
        return clockTimestamp[0];
    }
}
