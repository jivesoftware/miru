package com.jivesoftware.os.miru.service.stream.allocator;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;

/**
 *
 */
public class LongIntKeyValueMarshaller implements KeyValueMarshaller<Long, Integer> {

    @Override
    public byte[] keyBytes(Long key) {
        return FilerIO.longBytes(key);
    }

    @Override
    public Long bytesKey(byte[] keyBytes, int offset) {
        return FilerIO.bytesLong(keyBytes, offset);
    }
    @Override
    public Integer bytesValue(Long key, byte[] valueBytes, int offset) {
        return FilerIO.bytesInt(valueBytes, offset);
    }

    @Override
    public byte[] valueBytes(Integer value) {
        return FilerIO.intBytes(value);
    }

}
