package com.jivesoftware.os.miru.cluster.marshaller;

import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.UtilLexMarshaller;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.cluster.rcvs.MiruTopologyColumnKey;
import java.nio.ByteBuffer;

/**
 *
 */
public class MiruTopologyColumnKeyMarshaller implements TypeMarshaller<MiruTopologyColumnKey> {

    private final MiruHostMarshaller hostMarshaller = new MiruHostMarshaller();

    @Override
    public MiruTopologyColumnKey fromBytes(byte[] bytes) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int partitionId = buffer.getInt();

        byte[] hostBytes = new byte[buffer.remaining()];
        buffer.get(hostBytes);

        return new MiruTopologyColumnKey(MiruPartitionId.of(partitionId), hostMarshaller.fromBytes(hostBytes));
    }

    @Override
    public byte[] toBytes(MiruTopologyColumnKey miruTopologyColumnKey) throws Exception {
        byte[] hostBytes = miruTopologyColumnKey.host != null ? hostMarshaller.toBytes(miruTopologyColumnKey.host) : new byte[0];

        int capacity = 4 + hostBytes.length; // partition (4) + host
        ByteBuffer buffer = ByteBuffer.allocate(capacity);

        if (miruTopologyColumnKey.partitionId != null) {
            buffer.putInt(miruTopologyColumnKey.partitionId.getId());
        } else {
            buffer.putInt(0);
        }
        buffer.put(hostBytes);

        return buffer.array();
    }

    @Override
    public MiruTopologyColumnKey fromLexBytes(byte[] bytes) throws Exception {
        int partitionId = UtilLexMarshaller.intFromLex(bytes, 0);

        byte[] hostBytes = new byte[bytes.length - 4];
        System.arraycopy(bytes, 4, hostBytes, 0, hostBytes.length);

        return new MiruTopologyColumnKey(MiruPartitionId.of(partitionId), hostMarshaller.fromLexBytes(hostBytes));
    }

    @Override
    public byte[] toLexBytes(MiruTopologyColumnKey miruTopologyColumnKey) throws Exception {
        byte[] hostBytes = miruTopologyColumnKey.host != null ? hostMarshaller.toLexBytes(miruTopologyColumnKey.host) : new byte[0];

        int capacity = 4 + hostBytes.length; // partition (4) + host
        ByteBuffer buffer = ByteBuffer.allocate(capacity);

        if (miruTopologyColumnKey.partitionId != null) {
            buffer.put(UtilLexMarshaller.intToLex(miruTopologyColumnKey.partitionId.getId()));
        } else {
            buffer.put(UtilLexMarshaller.intToLex(0));
        }
        buffer.put(hostBytes);

        return buffer.array();
    }
}
