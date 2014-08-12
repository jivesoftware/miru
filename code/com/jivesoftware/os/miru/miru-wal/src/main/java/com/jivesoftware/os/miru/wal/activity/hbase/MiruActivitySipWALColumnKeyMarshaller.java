package com.jivesoftware.os.miru.wal.activity.hbase;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.UtilLexMarshaller;
import java.nio.ByteBuffer;

public class MiruActivitySipWALColumnKeyMarshaller implements TypeMarshaller<MiruActivitySipWALColumnKey> {

    @Override
    public MiruActivitySipWALColumnKey fromBytes(byte[] bytes) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        byte sort = buffer.get();
        long collisionId = buffer.getLong();
        long sipId = buffer.getLong();

        return new MiruActivitySipWALColumnKey(sort, collisionId, sipId);
    }

    @Override
    public byte[] toBytes(MiruActivitySipWALColumnKey miruActivitySipWALColumnKey) throws Exception {
        Optional<Long> sipId = miruActivitySipWALColumnKey.getSipId();
        int capacity = 17; // sort (1 byte) + collisionId (8 bytes) + sipId (8 bytes)

        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.put(miruActivitySipWALColumnKey.getSort());
        buffer.putLong(miruActivitySipWALColumnKey.getCollisionId());

        if (sipId.isPresent()) {
            buffer.putLong(sipId.get());
        } else {
            buffer.putLong(Long.MAX_VALUE);
        }

        return buffer.array();
    }

    @Override
    public MiruActivitySipWALColumnKey fromLexBytes(byte[] bytes) throws Exception {
        byte sort = bytes[0];
        long collisionId = UtilLexMarshaller.longFromLex(bytes, 1);
        long sipId = UtilLexMarshaller.longFromLex(bytes, 9);

        return new MiruActivitySipWALColumnKey(sort, collisionId, sipId);
    }

    @Override
    public byte[] toLexBytes(MiruActivitySipWALColumnKey miruActivitySipWALColumnKey) throws Exception {
        Optional<Long> sipId = miruActivitySipWALColumnKey.getSipId();
        int capacity = 17; // sort (1 byte) + collisionId (8 bytes) + sipId (8 bytes)

        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.put(miruActivitySipWALColumnKey.getSort());
        buffer.put(UtilLexMarshaller.longToLex(miruActivitySipWALColumnKey.getCollisionId()));

        if (sipId.isPresent()) {
            buffer.put(UtilLexMarshaller.longToLex(sipId.get()));
        } else {
            buffer.put(UtilLexMarshaller.longToLex(Long.MAX_VALUE));
        }

        return buffer.array();
    }
}
