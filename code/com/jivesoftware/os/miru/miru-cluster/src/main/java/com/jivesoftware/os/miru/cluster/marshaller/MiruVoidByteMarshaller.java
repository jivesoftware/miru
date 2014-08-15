package com.jivesoftware.os.miru.cluster.marshaller;

import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;
import com.jivesoftware.os.miru.cluster.rcvs.MiruVoidByte;

/**
 *
 */
public class MiruVoidByteMarshaller implements TypeMarshaller<MiruVoidByte> {

    @Override
    public MiruVoidByte fromBytes(byte[] bytes) throws Exception {
        return fromLexBytes(bytes);
    }

    @Override
    public byte[] toBytes(MiruVoidByte miruVoidByte) throws Exception {
        return toLexBytes(miruVoidByte);
    }

    @Override
    public MiruVoidByte fromLexBytes(byte[] bytes) throws Exception {
        return MiruVoidByte.INSTANCE;
    }

    @Override
    public byte[] toLexBytes(MiruVoidByte miruVoidByte) throws Exception {
        return new byte[] { 0 };
    }
}
