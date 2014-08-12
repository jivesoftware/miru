package com.jivesoftware.os.miru.cluster.marshaller;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.cluster.rcvs.MiruActivityLookupEntry;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;
import java.nio.ByteBuffer;

/**
 *
 */
public class MiruActivityLookupEntryMarshaller implements TypeMarshaller<MiruActivityLookupEntry> {

    @Override
    public MiruActivityLookupEntry fromBytes(byte[] bytes) throws Exception {
        return fromLexBytes(bytes);
    }

    @Override
    public byte[] toBytes(MiruActivityLookupEntry entry) throws Exception {
        return toLexBytes(entry);
    }

    @Override
    public MiruActivityLookupEntry fromLexBytes(byte[] bytes) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int partitionId = buffer.getInt();
        int index = buffer.getInt();
        int writerId = buffer.getInt();
        boolean removed = (buffer.get() == 1);

        return new MiruActivityLookupEntry(partitionId, index, writerId, removed);
    }

    @Override
    public byte[] toLexBytes(MiruActivityLookupEntry entry) throws Exception {
        Preconditions.checkNotNull(entry);
        ByteBuffer buffer = ByteBuffer.allocate(13);

        buffer.putInt(entry.partitionId);
        buffer.putInt(entry.index);
        buffer.putInt(entry.writerId);
        buffer.put((byte) (entry.removed ? 1 : 0));

        return buffer.array();
    }
}
