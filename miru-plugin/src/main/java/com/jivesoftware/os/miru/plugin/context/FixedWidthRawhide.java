package com.jivesoftware.os.miru.plugin.context;

import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.guts.IndexUtil;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.lab.io.api.UIO;

/**
 * @author jonathan.colt
 */
public class FixedWidthRawhide implements Rawhide {

    private final int keyLength;
    private final int payloadLength;

    public FixedWidthRawhide(int keyLength, int payloadLength) {
        this.keyLength = keyLength;
        this.payloadLength = payloadLength;
    }

    @Override
    public byte[] merge(byte[] current, byte[] adding) {
        return adding;
    }

    @Override
    public boolean streamRawEntry(ValueStream stream, byte[] rawEntry, int offset) throws Exception {
        if (rawEntry == null) {
            return stream.stream(null, -1, false, -1, null);
        }
        byte[] key = new byte[keyLength];
        UIO.readBytes(rawEntry, 0, key);
        byte[] payload = new byte[payloadLength];
        UIO.readBytes(rawEntry, keyLength, payload);
        return stream.stream(key, 0, false, 0, payload);
    }

    @Override
    public byte[] toRawEntry(byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) throws Exception {
        return payload != null ? UIO.add(key, payload) : key;
    }

    @Override
    public int entryLength(IReadable readable, byte[] lengthBuffer) throws Exception {
        return keyLength + payloadLength;
    }

    @Override
    public void writeRawEntry(byte[] rawEntry, int offset, int length, IAppendOnly appendOnly, byte[] lengthBuffer) throws Exception {
        appendOnly.append(rawEntry, offset, length);
    }

    @Override
    public byte[] key(byte[] rawEntry, int offset, int length) throws Exception {
        byte[] key = new byte[keyLength];
        UIO.readBytes(rawEntry, 0, key);
        return key;
    }

    @Override
    public int keyLength(byte[] rawEntry, int offset) {
        return keyLength;
    }

    @Override
    public int keyOffset(byte[] rawEntry, int offset) {
        return offset;
    }

    @Override
    public int compareKey(byte[] rawEntry, int offset, byte[] compareKey, int compareOffset, int compareLength) {
        return IndexUtil.compare(rawEntry, offset, keyLength, compareKey, compareOffset, compareLength);
    }

    @Override
    public int compareKeyFromEntry(IReadable readable, byte[] compareKey, int compareOffset, int compareLength, byte[] intBuffer) throws Exception {
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
