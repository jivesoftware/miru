package com.jivesoftware.os.miru.wal.lookup;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.wal.MiruRangeLookupColumnKey;
import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;
import java.nio.ByteBuffer;

/**
 *
 */
public class MiruRangeLookupColumnKeyMarshaller implements TypeMarshaller<MiruRangeLookupColumnKey> {

    @Override
    public MiruRangeLookupColumnKey fromBytes(byte[] bytes) throws Exception {
        return fromLexBytes(bytes);
    }

    @Override
    public byte[] toBytes(MiruRangeLookupColumnKey key) throws Exception {
        return toLexBytes(key);
    }

    @Override
    public MiruRangeLookupColumnKey fromLexBytes(byte[] bytes) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int partitionId = buffer.getInt();
        return new MiruRangeLookupColumnKey(partitionId, buffer.get());
    }

    @Override
    public byte[] toLexBytes(MiruRangeLookupColumnKey key) throws Exception {
        Preconditions.checkNotNull(key);
        ByteBuffer buffer = ByteBuffer.allocate(5);

        buffer.putInt(key.partitionId);
        buffer.put(key.type);

        return buffer.array();
    }
}
