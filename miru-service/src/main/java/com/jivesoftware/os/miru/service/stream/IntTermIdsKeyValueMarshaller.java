package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.nio.ByteBuffer;

/**
 *
 */
public class IntTermIdsKeyValueMarshaller implements KeyValueMarshaller<Integer, MiruTermId[]> {

    @Override
    public byte[] keyBytes(Integer integer) {
        return FilerIO.intBytes(integer);
    }

    @Override
    public Integer bytesKey(byte[] bytes, int i) {
        return FilerIO.bytesInt(bytes);
    }

    @Override
    public byte[] valueBytes(MiruTermId[] termIds) {
        int size = valueSizeInBytes(termIds);

        byte[] bytes = new byte[size];
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        buf.putShort((short) termIds.length);

        for (MiruTermId termId : termIds) {
            buf.putShort((short) termId.length());
            buf.put(termId.getBytes());
        }
        return bytes;
    }

    @Override
    public MiruTermId[] bytesValue(Integer integer, byte[] bytes, int offset) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        MiruTermId[] termIds = new MiruTermId[buf.getShort()];
        for (int i = 0; i < termIds.length; i++) {
            int termIdLength = buf.getShort();
            byte[] termId = new byte[termIdLength];
            buf.get(termId);
            termIds[i] = new MiruTermId(termId);
        }
        return termIds;
    }

    public int valueSizeInBytes(MiruTermId[] termIds) {
        int size = 2;
        for (MiruTermId termId : termIds) {
            size += 2 + termId.length();
        }
        return size;
    }
}
