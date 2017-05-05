package com.jivesoftware.os.miru.wal.readtracking.amza;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.FailedToAchieveQuorumException;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream.TxResult;
import com.jivesoftware.os.amza.api.take.TakeCursors;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider.EmbeddedClient;
import com.jivesoftware.os.amza.service.PartitionIsDisposedException;
import com.jivesoftware.os.amza.service.PropertiesNotPresentException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.JacksonJsonObjectTypeMarshaller;
import com.jivesoftware.os.miru.api.topology.NamedCursor;
import com.jivesoftware.os.miru.api.wal.AmzaCursor;
import com.jivesoftware.os.miru.api.wal.AmzaSipCursor;
import com.jivesoftware.os.miru.wal.AmzaWALUtil;
import com.jivesoftware.os.miru.wal.MiruWALWrongRouteException;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AmzaReadTrackingWALReader implements MiruReadTrackingWALReader<AmzaCursor, AmzaSipCursor> {

    private final AmzaWALUtil amzaWALUtil;
    private final JacksonJsonObjectTypeMarshaller<MiruPartitionedActivity> partitionedActivityMarshaller;

    public AmzaReadTrackingWALReader(AmzaWALUtil amzaWALUtil, ObjectMapper mapper) {
        this.amzaWALUtil = amzaWALUtil;
        this.partitionedActivityMarshaller = new JacksonJsonObjectTypeMarshaller<>(MiruPartitionedActivity.class, mapper);
    }

    private Map<String, NamedCursor> extractCursors(List<NamedCursor> cursors) {
        Map<String, NamedCursor> cursorsByName = Maps.newHashMapWithExpectedSize(cursors.size());
        for (NamedCursor namedCursor : cursors) {
            cursorsByName.put(namedCursor.name, namedCursor);
        }
        return cursorsByName;
    }

    private long scanCursors(MiruReadTrackingWALReader.StreamReadTrackingWAL streamMiruReadTrackingWAL,
        EmbeddedClient client,
        MiruStreamId streamId,
        long id) throws Exception {
        return amzaWALUtil.scan(client, id, streamId.getBytes(),
            (byte[] prefix, byte[] key, byte[] value, long timestamp, long version) -> {
                MiruPartitionedActivity partitionedActivity = partitionedActivityMarshaller.fromBytes(value);
                if (partitionedActivity != null) {
                    if (!streamMiruReadTrackingWAL.stream(partitionedActivity.timestamp, partitionedActivity, timestamp)) {
                        return false;
                    }
                }
                return true;
            });
    }

    private TakeCursors takeSipCursors(MiruReadTrackingWALReader.StreamReadTrackingSipWAL streamMiruReadTrackingSipWAL,
        EmbeddedClient client,
        MiruStreamId streamId,
        Map<String, NamedCursor> cursorsByName) throws Exception {
        return amzaWALUtil.take(client, cursorsByName, streamId.getBytes(),
            (long rowTxId, byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned, long valueVersion) -> {
                MiruPartitionedActivity partitionedActivity = partitionedActivityMarshaller.fromBytes(value);
                if (partitionedActivity != null) {
                    //TODO key->bytes is sufficient for the activity timestamp, so technically we don't need values at all
                    if (!streamMiruReadTrackingSipWAL.stream(partitionedActivity.timestamp, rowTxId)) {
                        return TxResult.ACCEPT_AND_STOP;
                    }
                }
                return TxResult.MORE;
            });
    }

    public HostPort[] getRoutingGroup(MiruTenantId tenantId, boolean createIfAbsent) throws Exception {
        return amzaWALUtil.getReadTrackingRoutingGroup(tenantId, Optional.absent(), createIfAbsent);
    }

    @Override
    public AmzaCursor getCursor(long eventId) {
        return new AmzaCursor(Collections.singletonList(new NamedCursor(amzaWALUtil.getRingMemberName(), eventId)), null);
    }

    @Override
    public long stream(MiruTenantId tenantId,
        MiruStreamId streamId,
        long id,
        StreamReadTrackingWAL streamReadTrackingWAL) throws Exception {

        EmbeddedClient client = amzaWALUtil.getReadTrackingClient(tenantId);
        if (client == null) {
            return id;
        }
        try {
            return scanCursors(streamReadTrackingWAL, client, streamId, id);
        } catch (PropertiesNotPresentException | PartitionIsDisposedException e) {
            return id;
        } catch (FailedToAchieveQuorumException e) {
            throw new MiruWALWrongRouteException(e);
        }
    }

    @Override
    public AmzaSipCursor streamSip(MiruTenantId tenantId,
        MiruStreamId streamId,
        AmzaSipCursor sipCursor,
        int batchSize,
        StreamReadTrackingSipWAL streamReadTrackingSipWAL) throws Exception {

        EmbeddedClient client = amzaWALUtil.getReadTrackingClient(tenantId);
        if (client == null) {
            return sipCursor;
        }
        try {
            Map<String, NamedCursor> sipCursorsByName = sipCursor != null ? extractCursors(sipCursor.cursors) : Maps.newHashMap();

            TakeCursors takeCursors = takeSipCursors(streamReadTrackingSipWAL, client, streamId, sipCursorsByName);

            amzaWALUtil.mergeCursors(sipCursorsByName, takeCursors);

            return new AmzaSipCursor(sipCursorsByName.values(), takeCursors.tookToEnd);
        } catch (PropertiesNotPresentException | PartitionIsDisposedException e) {
            return sipCursor;
        } catch (FailedToAchieveQuorumException e) {
            throw new MiruWALWrongRouteException(e);
        }
    }
}
