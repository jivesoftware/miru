package com.jivesoftware.os.miru.wal.activity.hbase;

import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;
import java.nio.ByteBuffer;

public class MiruActivityWALRowMarshaller implements TypeMarshaller<MiruActivityWALRow> {

    @Override
    public MiruActivityWALRow fromBytes(byte[] bytes) throws Exception {
        int partitionId = ByteBuffer.wrap(bytes).getInt();

        return new MiruActivityWALRow(partitionId);
    }

    @Override
    public byte[] toBytes(MiruActivityWALRow miruActivityWALRow) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(4);

        buffer.putInt(miruActivityWALRow.getPartitionId());

        return buffer.array();
    }

    @Override
    public MiruActivityWALRow fromLexBytes(byte[] bytes) throws Exception {
        return fromBytes(bytes);
    }

    @Override
    public byte[] toLexBytes(MiruActivityWALRow miruActivityWALRow) throws Exception {
        return toBytes(miruActivityWALRow);
    }
}
