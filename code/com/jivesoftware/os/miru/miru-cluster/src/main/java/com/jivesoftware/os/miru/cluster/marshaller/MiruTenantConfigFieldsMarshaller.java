package com.jivesoftware.os.miru.cluster.marshaller;

import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;
import com.jivesoftware.os.miru.cluster.MiruTenantConfigFields;
import java.nio.ByteBuffer;

/**
 *
 */
public class MiruTenantConfigFieldsMarshaller implements TypeMarshaller<MiruTenantConfigFields> {

    @Override
    public MiruTenantConfigFields fromBytes(byte[] bytes) throws Exception {
        return fromLexBytes(bytes);
    }

    @Override
    public byte[] toBytes(MiruTenantConfigFields miruTenantConfigFields) throws Exception {
        return toLexBytes(miruTenantConfigFields);
    }

    @Override
    public MiruTenantConfigFields fromLexBytes(byte[] bytes) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int index = buffer.getInt();
        return MiruTenantConfigFields.fromIndex(index);
    }

    @Override
    public byte[] toLexBytes(MiruTenantConfigFields miruTenantConfigFields) throws Exception {
        int capacity = 4; // index (4 bytes)
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.putInt(miruTenantConfigFields.getIndex());
        return buffer.array();
    }
}
