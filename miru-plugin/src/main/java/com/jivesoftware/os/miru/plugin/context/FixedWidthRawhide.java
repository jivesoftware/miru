package com.jivesoftware.os.miru.plugin.context;

import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.lab.io.api.UIO;

/**
 *
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
        return UIO.add(key, payload);
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
        return false;
    }
}
