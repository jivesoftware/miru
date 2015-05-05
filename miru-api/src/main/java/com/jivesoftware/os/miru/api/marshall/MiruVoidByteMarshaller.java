package com.jivesoftware.os.miru.api.marshall;

import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;

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
