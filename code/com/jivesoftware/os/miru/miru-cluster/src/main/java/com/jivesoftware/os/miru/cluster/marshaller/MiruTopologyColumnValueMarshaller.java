package com.jivesoftware.os.miru.cluster.marshaller;

import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.cluster.rcvs.MiruTopologyColumnValue;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;
import java.nio.ByteBuffer;

/**
 *
 */
public class MiruTopologyColumnValueMarshaller implements TypeMarshaller<MiruTopologyColumnValue> {

    @Override
    public MiruTopologyColumnValue fromBytes(byte[] bytes) throws Exception {
        return fromLexBytes(bytes);
    }

    @Override
    public byte[] toBytes(MiruTopologyColumnValue miruTopologyColumnValue) throws Exception {
        return toLexBytes(miruTopologyColumnValue);
    }

    @Override
    public MiruTopologyColumnValue fromLexBytes(byte[] bytes) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int stateIndex = buffer.getInt();
        int storageIndex = buffer.getInt();

        long sizeInMemory = -1;
        long sizeOnDisk = -1;
        if (buffer.limit() >= 24) {
            sizeInMemory = buffer.getLong();
            sizeOnDisk = buffer.getLong();
        }

        return new MiruTopologyColumnValue(MiruPartitionState.fromIndex(stateIndex), MiruBackingStorage.fromIndex(storageIndex), sizeInMemory, sizeOnDisk);
    }

    @Override
    public byte[] toLexBytes(MiruTopologyColumnValue miruTopologyColumnValue) throws Exception {
        int capacity = 24;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);

        buffer.putInt(miruTopologyColumnValue.state.getIndex());
        buffer.putInt(miruTopologyColumnValue.storage.getIndex());
        buffer.putLong(miruTopologyColumnValue.sizeInMemory);
        buffer.putLong(miruTopologyColumnValue.sizeOnDisk);

        return buffer.array();
    }
}
