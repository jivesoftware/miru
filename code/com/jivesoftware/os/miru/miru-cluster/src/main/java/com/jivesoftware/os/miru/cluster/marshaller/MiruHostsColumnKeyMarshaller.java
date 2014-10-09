package com.jivesoftware.os.miru.cluster.marshaller;

import com.jivesoftware.os.miru.cluster.rcvs.MiruHostsColumnKey;
import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;
import java.nio.ByteBuffer;

/**
 *
 */
public class MiruHostsColumnKeyMarshaller implements TypeMarshaller<MiruHostsColumnKey> {

    @Override
    public MiruHostsColumnKey fromBytes(byte[] bytes) throws Exception {
        return fromLexBytes(bytes);
    }

    @Override
    public byte[] toBytes(MiruHostsColumnKey miruHostsColumnKey) throws Exception {
        return toLexBytes(miruHostsColumnKey);
    }

    @Override
    public MiruHostsColumnKey fromLexBytes(byte[] bytes) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int index = buffer.getInt();
        return MiruHostsColumnKey.fromIndex(index);
    }

    @Override
    public byte[] toLexBytes(MiruHostsColumnKey miruHostsColumnKey) throws Exception {
        int capacity = 4; // index (4 bytes)
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.putInt(miruHostsColumnKey.getIndex());
        return buffer.array();
    }
}
