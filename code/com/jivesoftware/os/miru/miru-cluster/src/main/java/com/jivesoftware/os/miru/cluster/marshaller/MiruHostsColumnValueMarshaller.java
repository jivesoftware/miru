package com.jivesoftware.os.miru.cluster.marshaller;

import com.jivesoftware.os.miru.cluster.rcvs.MiruHostsColumnValue;
import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;
import java.nio.ByteBuffer;

/**
 *
 */
public class MiruHostsColumnValueMarshaller implements TypeMarshaller<MiruHostsColumnValue> {

    @Override
    public MiruHostsColumnValue fromBytes(byte[] bytes) throws Exception {
        return fromLexBytes(bytes);
    }

    @Override
    public byte[] toBytes(MiruHostsColumnValue miruHostsColumnValue) throws Exception {
        return toLexBytes(miruHostsColumnValue);
    }

    @Override
    public MiruHostsColumnValue fromLexBytes(byte[] bytes) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        buffer.getLong(); // This is unused space from when we had sizeInMemory
        buffer.getLong(); // This is unused space from when we had sizeOnDisk

        return new MiruHostsColumnValue();
    }

    @Override
    public byte[] toLexBytes(MiruHostsColumnValue miruHostsColumnValue) throws Exception {
        int capacity = 16;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);

        buffer.putLong(-1); // This is unused space from when we had sizeInMemory
        buffer.putLong(-1); // This is unused space from when we had sizeOnDisk

        return buffer.array();
    }
}
