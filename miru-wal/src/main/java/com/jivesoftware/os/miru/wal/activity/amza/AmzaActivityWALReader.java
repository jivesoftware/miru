package com.jivesoftware.os.miru.wal.activity.amza;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.client.AmzaKretrProvider.AmzaKretr;
import com.jivesoftware.os.amza.shared.take.TakeCursors;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.TenantAndPartition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.JacksonJsonObjectTypeMarshaller;
import com.jivesoftware.os.miru.api.topology.NamedCursor;
import com.jivesoftware.os.miru.api.wal.AmzaCursor;
import com.jivesoftware.os.miru.api.wal.AmzaSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.WriterCursor;
import com.jivesoftware.os.miru.wal.AmzaWALUtil;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKeyMarshaller;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 */
public class AmzaActivityWALReader implements MiruActivityWALReader<AmzaCursor, AmzaSipCursor> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaWALUtil amzaWALUtil;
    private final MiruActivityWALColumnKeyMarshaller columnKeyMarshaller = new MiruActivityWALColumnKeyMarshaller();
    private final JacksonJsonObjectTypeMarshaller<MiruPartitionedActivity> partitionedActivityMarshaller;

    public AmzaActivityWALReader(AmzaWALUtil amzaWALUtil, ObjectMapper mapper) {
        this.amzaWALUtil = amzaWALUtil;
        this.partitionedActivityMarshaller = new JacksonJsonObjectTypeMarshaller<>(MiruPartitionedActivity.class, mapper);
    }

    private void mergeCursors(Map<String, NamedCursor> cursorsByName, TakeCursors takeCursors) {
        for (TakeCursors.RingMemberCursor memberCursor : takeCursors.ringMemberCursors) {
            String memberName = memberCursor.ringMember.getMember();
            NamedCursor existing = cursorsByName.get(memberName);
            if (existing == null || memberCursor.transactionId > existing.id) {
                cursorsByName.put(memberName, new NamedCursor(memberName, memberCursor.transactionId));
            }
        }
    }

    private Map<String, NamedCursor> extractCursors(List<NamedCursor> cursors) {
        Map<String, NamedCursor> cursorsByName = Maps.newHashMap();
        for (NamedCursor namedCursor : cursors) {
            cursorsByName.put(namedCursor.name, namedCursor);
        }
        return cursorsByName;
    }

    private TakeCursors takeCursors(StreamMiruActivityWAL streamMiruActivityWAL,
        AmzaKretr client,
        Map<String, NamedCursor> cursorsByName) throws Exception {
        return amzaWALUtil.take(client, cursorsByName, (rowTxId, key, value) -> {
            MiruPartitionedActivity partitionedActivity = partitionedActivityMarshaller.fromBytes(value.getValue());
            if (partitionedActivity != null) {
                streamMiruActivityWAL.stream(partitionedActivity.timestamp, partitionedActivity, value.getTimestampId());
            }
            return true;
        });
    }

    @Override
    public HostPort[] getRoutingGroup(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return amzaWALUtil.getActivityRoutingGroup(tenantId, partitionId);
    }

    @Override
    public AmzaCursor stream(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        AmzaCursor cursor,
        int batchSize,
        StreamMiruActivityWAL streamMiruActivityWAL) throws Exception {

        AmzaKretr client = amzaWALUtil.getActivityClient(tenantId, partitionId);
        if (client == null) {
            return cursor;
        }

        Map<String, NamedCursor> cursorsByName = cursor != null ? extractCursors(cursor.cursors) : Maps.newHashMap();
        Map<String, NamedCursor> sipCursorsByName = cursor != null && cursor.sipCursor != null
            ? extractCursors(cursor.sipCursor.cursors) : Maps.newHashMap();

        TakeCursors takeCursors = takeCursors(streamMiruActivityWAL, client, cursorsByName);

        mergeCursors(cursorsByName, takeCursors);
        mergeCursors(sipCursorsByName, takeCursors);

        return new AmzaCursor(cursorsByName.values(), new AmzaSipCursor(sipCursorsByName.values()));
    }

    @Override
    public AmzaSipCursor streamSip(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        AmzaSipCursor sipCursor,
        int batchSize,
        StreamMiruActivityWAL streamMiruActivityWAL) throws Exception {

        AmzaKretr client = amzaWALUtil.getActivityClient(tenantId, partitionId);
        if (client == null) {
            return sipCursor;
        }

        Map<String, NamedCursor> sipCursorsByName = sipCursor != null ? extractCursors(sipCursor.cursors) : Maps.newHashMap();

        TakeCursors takeCursors = takeCursors(streamMiruActivityWAL, client, sipCursorsByName);

        mergeCursors(sipCursorsByName, takeCursors);

        return new AmzaSipCursor(sipCursorsByName.values());
    }

    @Override
    public WriterCursor getCursorForWriterId(MiruTenantId tenantId, int writerId) throws Exception {
        LOG.inc("getCursorForWriterId");
        LOG.inc("getCursorForWriterId>" + writerId, tenantId.toString());
        MiruPartitionId partitionId = MiruPartitionId.of(0);
        int largestRunOfGaps = 10; // TODO expose to config?
        MiruPartitionedActivity latestBoundaryActivity = null;
        int gaps = 0;
        while (true) {
            AmzaKretr client = amzaWALUtil.getActivityClient(tenantId, partitionId);
            MiruPartitionedActivity got = null;
            if (client != null) {
                WALKey key = new WALKey(
                    columnKeyMarshaller.toLexBytes(new MiruActivityWALColumnKey(MiruPartitionedActivity.Type.BEGIN.getSort(), (long) writerId)));
                byte[] valueBytes = client.getValue(key);
                if (valueBytes != null) {
                    got = partitionedActivityMarshaller.fromBytes(valueBytes);
                }
            }
            if (got != null) {
                gaps = 0;
                latestBoundaryActivity = got;
            } else {
                gaps++;
                if (gaps > largestRunOfGaps) {
                    break;
                }
            }
            partitionId = partitionId.next();
        }

        if (latestBoundaryActivity == null) {
            return new WriterCursor(0, 0);
        } else {
            return new WriterCursor(latestBoundaryActivity.getPartitionId(), latestBoundaryActivity.index);
        }
    }

    @Override
    public MiruPartitionId largestPartitionId(MiruTenantId tenantId) throws Exception {
        LOG.inc("largestPartitionId");
        MiruPartitionId partitionId = MiruPartitionId.of(0);
        int largestRunOfGaps = 10; // TODO expose to config?
        MiruPartitionId latestPartitionId = partitionId;
        int gaps = 0;
        while (true) {
            boolean got = amzaWALUtil.hasActivityPartition(tenantId, partitionId);
            if (got) {
                gaps = 0;
                latestPartitionId = partitionId;
            } else {
                gaps++;
                if (gaps > largestRunOfGaps) {
                    break;
                }
            }
            partitionId = partitionId.next();
        }

        return latestPartitionId;
    }

    @Override
    public MiruActivityWALStatus getStatus(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        final MutableLong count = new MutableLong(0);
        final List<Integer> begins = Lists.newArrayList();
        final List<Integer> ends = Lists.newArrayList();
        AmzaKretr client = amzaWALUtil.getActivityClient(tenantId, partitionId);
        if (client != null) {
            WALKey fromKey = new WALKey(
                columnKeyMarshaller.toLexBytes(new MiruActivityWALColumnKey(MiruPartitionedActivity.Type.END.getSort(), 0)));
            WALKey toKey = new WALKey(
                columnKeyMarshaller.toLexBytes(new MiruActivityWALColumnKey(MiruPartitionedActivity.Type.BEGIN.getSort(), Long.MAX_VALUE)));
            client.scan(fromKey, toKey, (rowTxId, key, value) -> {
                if (value != null) {
                    MiruPartitionedActivity partitionedActivity = partitionedActivityMarshaller.fromBytes(value.getValue());
                    if (partitionedActivity.type == MiruPartitionedActivity.Type.BEGIN) {
                        count.add(partitionedActivity.index);
                        begins.add(partitionedActivity.writerId);
                    } else if (partitionedActivity.type == MiruPartitionedActivity.Type.END) {
                        ends.add(partitionedActivity.writerId);
                    }
                }
                return true;
            });
        }
        return new MiruActivityWALStatus(partitionId, count.longValue(), begins, ends);
    }

    @Override
    public long oldestActivityClockTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        final MutableLong oldestClockTimestamp = new MutableLong(-1);
        AmzaKretr client = amzaWALUtil.getActivityClient(tenantId, partitionId);
        if (client != null) {
            WALKey fromKey = new WALKey(columnKeyMarshaller.toBytes(new MiruActivityWALColumnKey(MiruPartitionedActivity.Type.ACTIVITY.getSort(), 0L)));
            WALKey toKey = new WALKey(columnKeyMarshaller.toBytes(new MiruActivityWALColumnKey(MiruPartitionedActivity.Type.END.getSort(), 0L)));
            client.scan(fromKey, toKey, (rowTxId, key, value) -> {
                if (value != null) {
                    MiruPartitionedActivity partitionedActivity = partitionedActivityMarshaller.fromBytes(value.getValue());
                    if (partitionedActivity != null && partitionedActivity.type.isActivityType()) {
                        oldestClockTimestamp.setValue(partitionedActivity.clockTimestamp);
                        return false;
                    }
                }
                return true;
            });
        }
        return oldestClockTimestamp.longValue();
    }

    @Override
    public void delete(MiruTenantId tenantId, MiruPartitionId partitionId, Collection<MiruActivityWALColumnKey> keys) throws Exception {
        throw new UnsupportedOperationException("You wish");
    }

    @Override
    public void deleteSip(MiruTenantId tenantId, MiruPartitionId partitionId, Collection<MiruActivitySipWALColumnKey> keys) throws Exception {
        throw new UnsupportedOperationException("You wish");
    }

    @Override
    public void allPartitions(PartitionsStream stream) throws Exception {
        AmzaKretr partitionsClient = amzaWALUtil.getLookupPartitionsClient();
        partitionsClient.scan(null, null, (rowTxId, key, scanned) -> {
            if (key != null) {
                TenantAndPartition tenantAndPartition = amzaWALUtil.fromPartitionsKey(key);
                if (!stream.stream(tenantAndPartition.tenantId, tenantAndPartition.partitionId)) {
                    return false;
                }
            }
            return true;
        });
    }
}
