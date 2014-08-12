package com.jivesoftware.os.miru.cluster.marshaller;

import com.jivesoftware.os.miru.cluster.rcvs.MiruHostsColumnValue;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;
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

        long sizeInMemory = -1;
        if (buffer.limit() >= 8) {
            sizeInMemory = buffer.getLong();
        }

        long sizeOnDisk = -1;
        if (buffer.limit() >= 16) {
            sizeOnDisk = buffer.getLong();
        }

        return new MiruHostsColumnValue(sizeInMemory, sizeOnDisk);
    }

    @Override
    public byte[] toLexBytes(MiruHostsColumnValue miruHostsColumnValue) throws Exception {
        int capacity = 16;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);

        buffer.putLong(miruHostsColumnValue.sizeInMemory);
        buffer.putLong(miruHostsColumnValue.sizeOnDisk);

        return buffer.array();
    }
}
