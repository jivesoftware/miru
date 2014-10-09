package com.jivesoftware.os.miru.cluster.marshaller;

import com.jivesoftware.os.miru.cluster.rcvs.MiruSchemaColumnKey;
import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;
import java.nio.ByteBuffer;

/**
 *
 */
public class MiruSchemaColumnKeyMarshaller implements TypeMarshaller<MiruSchemaColumnKey> {

    @Override
    public MiruSchemaColumnKey fromBytes(byte[] bytes) throws Exception {
        return fromLexBytes(bytes);
    }

    @Override
    public byte[] toBytes(MiruSchemaColumnKey miruSchemaColumnKey) throws Exception {
        return toLexBytes(miruSchemaColumnKey);
    }

    @Override
    public MiruSchemaColumnKey fromLexBytes(byte[] bytes) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int index = buffer.getInt();
        return MiruSchemaColumnKey.fromIndex(index);
    }

    @Override
    public byte[] toLexBytes(MiruSchemaColumnKey miruSchemaColumnKey) throws Exception {
        int capacity = 4; // index (4 bytes)
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.putInt(miruSchemaColumnKey.getIndex());
        return buffer.array();
    }
}
