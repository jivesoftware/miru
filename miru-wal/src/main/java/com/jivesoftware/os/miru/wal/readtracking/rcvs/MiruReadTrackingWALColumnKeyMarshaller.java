package com.jivesoftware.os.miru.wal.readtracking.rcvs;

import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;
import com.jivesoftware.os.rcvs.marshall.api.UtilLexMarshaller;
import java.nio.ByteBuffer;

public class MiruReadTrackingWALColumnKeyMarshaller implements TypeMarshaller<MiruReadTrackingWALColumnKey> {

    @Override
    public MiruReadTrackingWALColumnKey fromBytes(byte[] bytes) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return new MiruReadTrackingWALColumnKey(buffer.getLong());
    }

    @Override
    public byte[] toBytes(MiruReadTrackingWALColumnKey miruReadTrackingWALColumnKey) throws Exception {
        int capacity = 8; // eventId (8 bytes)

        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.putLong(miruReadTrackingWALColumnKey.getEventId());
        return buffer.array();
    }

    @Override
    public MiruReadTrackingWALColumnKey fromLexBytes(byte[] bytes) throws Exception {
        long eventId = UtilLexMarshaller.longFromLex(bytes);

        return new MiruReadTrackingWALColumnKey(eventId);
    }

    @Override
    public byte[] toLexBytes(MiruReadTrackingWALColumnKey miruReadTrackingWALColumnKey) throws Exception {
        int capacity = 8; // eventId (8 bytes)

        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.put(UtilLexMarshaller.longToLex(miruReadTrackingWALColumnKey.getEventId()));

        return buffer.array();
    }
}
