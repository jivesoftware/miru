package com.jivesoftware.os.miru.cluster.marshaller;

import com.jivesoftware.os.miru.cluster.rcvs.MiruSchemaColumnKey;
import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;
import com.jivesoftware.os.rcvs.marshall.api.UtilLexMarshaller;
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
        long version = Long.MAX_VALUE - UtilLexMarshaller.longFromLex(bytes, 0);
        int nameLen = UtilLexMarshaller.bytesInt(bytes, 8);
        String name = UtilLexMarshaller.stringFromLex(bytes, 12, nameLen);
        return new MiruSchemaColumnKey(name, version);
    }

    @Override
    public byte[] toLexBytes(MiruSchemaColumnKey miruSchemaColumnKey) throws Exception {
        byte[] nameBytes = UtilLexMarshaller.stringToLex(miruSchemaColumnKey.name);

        int capacity = 8 + 4 + nameBytes.length; // version (8) + nameLen (4) + name
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.put(UtilLexMarshaller.longToLex(Long.MAX_VALUE - miruSchemaColumnKey.version));
        buffer.put(UtilLexMarshaller.intBytes(nameBytes.length, new byte[4], 0));
        buffer.put(nameBytes);
        return buffer.array();
    }
}
