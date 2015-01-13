package com.jivesoftware.os.miru.cluster.marshaller;

import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.cluster.rcvs.MiruTopologyColumnValue;
import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;
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

        buffer.getLong(); // This is unused space from when we had sizeInMemory
        buffer.getLong(); // This is unused space from when we had sizeOnDisk

        long lastActiveTimestamp = -1;
        if (buffer.remaining() >= 8) {
            lastActiveTimestamp = buffer.getLong();
        }

        return new MiruTopologyColumnValue(MiruPartitionState.fromIndex(stateIndex), MiruBackingStorage.fromIndex(storageIndex), lastActiveTimestamp);
    }

    @Override
    public byte[] toLexBytes(MiruTopologyColumnValue miruTopologyColumnValue) throws Exception {
        int capacity = 32;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);

        buffer.putInt(miruTopologyColumnValue.state.getIndex());
        buffer.putInt(miruTopologyColumnValue.storage.getIndex());
        buffer.putLong(-1); // This is unused space from when we had sizeInMemory
        buffer.putLong(-1); // This is unused space from when we had sizeOnDisk
        buffer.putLong(miruTopologyColumnValue.lastActiveTimestamp);

        return buffer.array();
    }
}
