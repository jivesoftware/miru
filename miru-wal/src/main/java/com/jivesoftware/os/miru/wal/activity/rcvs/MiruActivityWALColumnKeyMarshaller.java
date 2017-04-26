package com.jivesoftware.os.miru.wal.activity.rcvs;

import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;
import com.jivesoftware.os.rcvs.marshall.api.UtilLexMarshaller;
import java.nio.ByteBuffer;

public class MiruActivityWALColumnKeyMarshaller implements TypeMarshaller<MiruActivityWALColumnKey> {

    public static void main(String[] args) {
        System.out.println(Long.toHexString(446611203385475072L).toUpperCase());
        System.out.println(Long.toHexString(509896568015429636L).toUpperCase());
    }

    @Override
    public MiruActivityWALColumnKey fromBytes(byte[] bytes) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        byte sort = buffer.get();
        long collisionId = buffer.getLong();

        return new MiruActivityWALColumnKey(sort, collisionId);
    }

    @Override
    public byte[] toBytes(MiruActivityWALColumnKey miruActivityWALColumnKey) throws Exception {
        int capacity = 9; // sort (1 byte) + collisionId (8 bytes)

        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.put(miruActivityWALColumnKey.getSort());
        buffer.putLong(miruActivityWALColumnKey.getCollisionId());
        return buffer.array();
    }

    @Override
    public MiruActivityWALColumnKey fromLexBytes(byte[] bytes) throws Exception {
        byte sort = bytes[0];
        long collisionId = UtilLexMarshaller.longFromLex(bytes, 1);

        return new MiruActivityWALColumnKey(sort, collisionId);
    }

    @Override
    public byte[] toLexBytes(MiruActivityWALColumnKey miruActivityWALColumnKey) throws Exception {
        int capacity = 9; // sort (1 byte) + collisionId (8 bytes)

        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.put(miruActivityWALColumnKey.getSort());
        buffer.put(UtilLexMarshaller.longToLex(miruActivityWALColumnKey.getCollisionId()));

        return buffer.array();
    }

    public byte getSort(byte[] key) {
        return key[0];
    }
}
