package com.jivesoftware.os.miru.cluster.marshaller;

import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionReplicaInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;
import java.nio.ByteBuffer;

/**
 *
 */
public class MiruPartitionReplicaInfoMarshaller implements TypeMarshaller<MiruPartitionReplicaInfo> {

    @Override
    public MiruPartitionReplicaInfo fromBytes(byte[] bytes) throws Exception {
        return fromLexBytes(bytes);
    }

    @Override
    public byte[] toBytes(MiruPartitionReplicaInfo value) throws Exception {
        return toLexBytes(value);
    }

    @Override
    public MiruPartitionReplicaInfo fromLexBytes(byte[] bytes) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long electionTimestampMillis = buffer.getLong();
        long lastActiveTimestampMillis = buffer.getLong();
        int stateIndex = buffer.getInt();
        int storageIndex = buffer.getInt();
        return new MiruPartitionReplicaInfo(
            electionTimestampMillis,
            lastActiveTimestampMillis,
            MiruPartitionState.fromIndex(stateIndex),
            MiruBackingStorage.fromIndex(storageIndex));
    }

    @Override
    public byte[] toLexBytes(MiruPartitionReplicaInfo value) throws Exception {
        int capacity = (8 + 8 + 4 + 4);
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.putLong(value.electionTimestampMillis);
        buffer.putLong(value.lastActiveTimestampMillis);
        buffer.putInt(value.state.getIndex());
        buffer.putInt(value.storage.getIndex());
        return buffer.array();
    }
}
