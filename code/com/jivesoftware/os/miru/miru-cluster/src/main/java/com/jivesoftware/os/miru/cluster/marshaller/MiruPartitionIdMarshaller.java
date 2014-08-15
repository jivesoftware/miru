package com.jivesoftware.os.miru.cluster.marshaller;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.UtilLexMarshaller;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;

/**
 *
 */
public class MiruPartitionIdMarshaller implements TypeMarshaller<MiruPartitionId> {

    @Override
    public MiruPartitionId fromBytes(byte[] bytes) throws Exception {
        return MiruPartitionId.of(Ints.fromByteArray(bytes));
    }

    @Override
    public byte[] toBytes(MiruPartitionId partitionId) throws Exception {
        Preconditions.checkNotNull(partitionId);
        return Ints.toByteArray(partitionId.getId());
    }

    @Override
    public MiruPartitionId fromLexBytes(byte[] bytes) throws Exception {
        return MiruPartitionId.of(UtilLexMarshaller.intFromLex(bytes));
    }

    @Override
    public byte[] toLexBytes(MiruPartitionId partitionId) throws Exception {
        Preconditions.checkNotNull(partitionId);
        return UtilLexMarshaller.intToLex(partitionId.getId());
    }
}
