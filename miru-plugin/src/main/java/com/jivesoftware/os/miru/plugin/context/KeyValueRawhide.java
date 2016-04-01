package com.jivesoftware.os.miru.plugin.context;

import com.jivesoftware.os.lab.LABUtils;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.guts.IndexUtil;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.lab.io.api.UIO;

/**
 *
 * @author jonathan.colt
 */
public class KeyValueRawhide implements Rawhide {

    @Override
    public byte[] merge(byte[] current, byte[] adding) {
        return adding;
    }

    @Override
    public boolean streamRawEntry(ValueStream stream, int index, byte[] rawEntry, int offset) throws Exception {
        if (rawEntry == null) {
            return stream.stream(index, null, -1, false, -1, null);
        }
        int o = offset;
        byte[] key = LABUtils.readByteArray(rawEntry, o);
        o += 4 + (key != null ? key.length : 0);
        byte[] payload = LABUtils.readByteArray(rawEntry, o);
        return stream.stream(index, key, 0, false, 0, payload);
    }

    @Override
    public byte[] toRawEntry(byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) throws Exception {
        byte[] rawEntry = new byte[LABUtils.rawArrayLength(key) + LABUtils.rawArrayLength(payload)];
        int o = 0;
        o += LABUtils.writeByteArray(key, rawEntry, o);
        LABUtils.writeByteArray(payload, rawEntry, o);
        return rawEntry;
    }

    @Override
    public int entryLength(IReadable readable, byte[] lengthBuffer) throws Exception {
        return UIO.readInt(readable, "length", lengthBuffer);
    }

    @Override
    public void writeRawEntry(byte[] rawEntry, int offset, int length, IAppendOnly appendOnly, byte[] lengthBuffer) throws Exception {
        UIO.writeByteArray(appendOnly, rawEntry, offset, length, "entry", lengthBuffer);
    }

    @Override
    public byte[] key(byte[] rawEntry, int offset, int length) throws Exception {
        return LABUtils.readByteArray(rawEntry, offset);
    }

    @Override
    public int keyLength(byte[] rawEntry, int offset) {
        return UIO.bytesInt(rawEntry, offset);
    }

    @Override
    public int keyOffset(byte[] rawEntry, int offset) {
        return offset + 4;
    }

    @Override
    public int compareKey(byte[] rawEntry, int offset, byte[] compareKey, int compareOffset, int compareLength) {
        int keylength = UIO.bytesInt(rawEntry, offset);
        return IndexUtil.compare(rawEntry, offset + 4, keylength, compareKey, compareOffset, compareLength);
    }

    @Override
    public int compareKeyFromEntry(IReadable readable, byte[] compareKey, int compareOffset, int compareLength, byte[] intBuffer) throws Exception {
        readable.seek(readable.getFilePointer() + 4); // skip the entry length
        int keyLength = UIO.readInt(readable, "keyLength", intBuffer);
        return IndexUtil.compare(readable, keyLength, compareKey, compareOffset, compareLength);
    }

    @Override
    public long timestamp(byte[] rawEntry, int offset, int length) {
        return 0;
    }

    @Override
    public long version(byte[] rawEntry, int offset, int length) {
        return 0;
    }

    @Override
    public boolean isNewerThan(long timestamp, long timestampVersion, long newerThanTimestamp, long newerThanTimestampVersion) {
        return true;
    }

    @Override
    public boolean mightContain(long timestamp, long timestampVersion, long newerThanTimestamp, long newerThanTimestampVersion) {
        return (timestamp != -1 && timestampVersion != -1);
    }
}
