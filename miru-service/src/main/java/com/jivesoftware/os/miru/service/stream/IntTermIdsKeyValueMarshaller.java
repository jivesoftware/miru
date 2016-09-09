package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.ByteBuffer;

/**
 *
 */
public class IntTermIdsKeyValueMarshaller implements KeyValueMarshaller<Integer, MiruTermId[]> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final int MAX_TERM_ID_COUNT = 65_536;
    private static final int MAX_VALUE_SIZE = 1_024 * 1_024;

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
        buf.position(2);
        int termCount;
        for (termCount = 0; termCount < termIds.length && termCount < MAX_TERM_ID_COUNT; termCount++) {
            MiruTermId termId = termIds[termCount];
            int termSize = 2 + termId.length();
            if (buf.position() + termSize > size) {
                LOG.warn("Truncated terms, size: " + (buf.position() + termSize) + " > " + size);
                break;
            }
            writeUnsignedShort(buf, termId.length());
            buf.put(termId.getBytes());
        }
        if (termCount >= MAX_TERM_ID_COUNT) {
            LOG.warn("Truncated terms, count: " + termCount + " > " + MAX_TERM_ID_COUNT);
        }
        buf.position(0);
        writeUnsignedShort(buf, termCount);
        return bytes;
    }

    @Override
    public MiruTermId[] bytesValue(Integer integer, byte[] bytes, int offset) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        MiruTermId[] termIds = new MiruTermId[readUnsignedShort(buf)];
        for (int i = 0; i < termIds.length; i++) {
            int termIdLength = readUnsignedShort(buf);
            byte[] termId = new byte[termIdLength];
            buf.get(termId);
            termIds[i] = new MiruTermId(termId);
        }
        return termIds;
    }

    public int valueSizeInBytes(MiruTermId[] termIds) {
        int size = 2;
        for (int i = 0; i < termIds.length && i < MAX_TERM_ID_COUNT; i++) {
            int termSize = 2 + termIds[i].length();
            if (size + termSize > MAX_VALUE_SIZE) {
                break;
            }
            size += termSize;
        }
        return size;
    }


    private int readUnsignedShort(ByteBuffer buf) {
        int length = 0;
        length |= (buf.get() & 0xFF);
        length <<= 8;
        length |= (buf.get() & 0xFF);
        return length;
    }

    private void writeUnsignedShort(ByteBuffer buf, int value) {
        buf.put((byte) ((value >>> 8) & 0xFF));
        buf.put((byte) (value & 0xFF));
    }
}
