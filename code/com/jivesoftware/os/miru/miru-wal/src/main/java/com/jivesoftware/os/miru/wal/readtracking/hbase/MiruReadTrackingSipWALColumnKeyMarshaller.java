package com.jivesoftware.os.miru.wal.readtracking.hbase;

import com.google.common.base.Optional;
import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;
import com.jivesoftware.os.rcvs.marshall.api.UtilLexMarshaller;
import java.nio.ByteBuffer;

public class MiruReadTrackingSipWALColumnKeyMarshaller implements TypeMarshaller<MiruReadTrackingSipWALColumnKey> {

    @Override
    public MiruReadTrackingSipWALColumnKey fromBytes(byte[] bytes) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long sipId = buffer.getLong();
        long eventId = buffer.getLong();

        return new MiruReadTrackingSipWALColumnKey(sipId, eventId);
    }

    @Override
    public byte[] toBytes(MiruReadTrackingSipWALColumnKey miruReadTrackingWALColumnKey) throws Exception {
        Optional<Long> eventId = miruReadTrackingWALColumnKey.getEventId();
        int capacity = 16; // sipId (8 bytes) + eventId (8 bytes)

        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.putLong(miruReadTrackingWALColumnKey.getSipId());

        if (eventId.isPresent()) {
            buffer.putLong(eventId.get());
        } else {
            buffer.putLong(Long.MAX_VALUE);
        }

        return buffer.array();
    }

    @Override
    public MiruReadTrackingSipWALColumnKey fromLexBytes(byte[] bytes) throws Exception {
        long sipId = UtilLexMarshaller.longFromLex(bytes);
        long eventId = UtilLexMarshaller.longFromLex(bytes, 8);

        return new MiruReadTrackingSipWALColumnKey(sipId, eventId);
    }

    @Override
    public byte[] toLexBytes(MiruReadTrackingSipWALColumnKey miruReadTrackingWALColumnKey) throws Exception {
        Optional<Long> eventId = miruReadTrackingWALColumnKey.getEventId();
        int capacity = 16; // sipId (8 bytes) + eventId (8 bytes)

        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.put(UtilLexMarshaller.longToLex(miruReadTrackingWALColumnKey.getSipId()));

        if (eventId.isPresent()) {
            buffer.put(UtilLexMarshaller.longToLex(eventId.get()));
        } else {
            buffer.put(UtilLexMarshaller.longToLex(Long.MAX_VALUE));
        }

        return buffer.array();
    }
}
