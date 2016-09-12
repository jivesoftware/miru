package com.jivesoftware.os.miru.plugin.context;

import com.jivesoftware.os.lab.BolBuffer;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.guts.IndexUtil;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * @author jonathan.colt
 */
public class LastIdKeyValueRawhide implements Rawhide {

    @Override
    public BolBuffer merge(FormatTransformer currentReadKeyFormatTransormer,
        FormatTransformer currentReadValueFormatTransormer,
        BolBuffer currentRawEntry,
        FormatTransformer addingReadKeyFormatTransormer,
        FormatTransformer addingReadValueFormatTransormer,
        BolBuffer addingRawEntry,
        FormatTransformer mergedReadKeyFormatTransormer,
        FormatTransformer mergedReadValueFormatTransormer) {

        long currentsTimestamp = (long) currentRawEntry.getInt(currentRawEntry.length - 4);
        long addingsTimestamp = (long) addingRawEntry.getInt(addingRawEntry.length - 4);

        return (currentsTimestamp > addingsTimestamp) ? currentRawEntry : addingRawEntry;
    }

    @Override
    public int mergeCompare(FormatTransformer aReadKeyFormatTransormer, FormatTransformer aReadValueFormatTransormer, ByteBuffer aRawEntry,
        FormatTransformer bReadKeyFormatTransormer, FormatTransformer bReadValueFormatTransormer, ByteBuffer bRawEntry) {

        int c = compareKey(aReadKeyFormatTransormer, aReadValueFormatTransormer, aRawEntry,
            bReadKeyFormatTransormer, bReadValueFormatTransormer, bRawEntry);
        if (c != 0) {
            return c;
        }

        if (aRawEntry == null && bRawEntry == null) {
            return 0;
        } else if (aRawEntry == null) {
            bRawEntry.clear();
            return -bRawEntry.capacity();
        } else if (bRawEntry == null) {
            aRawEntry.clear();
            return aRawEntry.capacity();
        } else {
            aRawEntry.clear();
            bRawEntry.clear();

            long asTimestamp = (long) aRawEntry.getInt(aRawEntry.capacity() - 4);
            long bsTimestamp = (long) bRawEntry.getInt(bRawEntry.capacity() - 4);

            if (asTimestamp == bsTimestamp) {
                return 0;
            }
            if ((asTimestamp > bsTimestamp)) {
                return -1;
            } else {
                return 1;
            }
        }
    }

    @Override
    public long timestamp(FormatTransformer readKeyFormatTransormer, FormatTransformer readValueFormatTransormer, BolBuffer rawEntry) {
        return rawEntry.getInt(rawEntry.length - 4);
    }

    @Override
    public long version(FormatTransformer readKeyFormatTransormer, FormatTransformer readValueFormatTransormer, BolBuffer rawEntry) {
        return 0;
    }

    @Override
    public boolean streamRawEntry(int index,
        FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        ByteBuffer rawEntry,
        ValueStream stream,
        boolean hydrateValues) throws Exception {
        if (rawEntry == null) {
            return stream.stream(index, null, -1, false, -1, null);
        }

        rawEntry.clear();
        int keyLength = rawEntry.getInt();
        rawEntry.limit(4 + keyLength);
        ByteBuffer key = rawEntry.slice();

        rawEntry.clear();
        int payloadLength = rawEntry.getInt(4 + keyLength);
        int lastId = rawEntry.getInt(4 + keyLength + 4 + payloadLength - 4);

        ByteBuffer payload = null;
        if (hydrateValues) {
            rawEntry.position(4 + keyLength + 4);
            rawEntry.limit(4 + keyLength + 4 + payloadLength - 4);
            payload = rawEntry.slice();
        }
        return stream.stream(index, key, lastId, false, 0, payload);
    }

    @Override
    public BolBuffer toRawEntry(byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload, BolBuffer rawEntryBuffer) throws Exception {

        int keyLength = ((key != null) ? key.length : 0);
        int payloadLength = ((payload != null) ? payload.length : 0);
        rawEntryBuffer.allocate(4 + keyLength + 4 + payloadLength + 4);

        UIO.intBytes(keyLength, rawEntryBuffer.bytes, 0);
        if (keyLength > 0) {
            UIO.writeBytes(key, rawEntryBuffer.bytes, 4);
        }
        UIO.intBytes(payloadLength + 4, rawEntryBuffer.bytes, 4 + keyLength);
        if (payloadLength > 0) {
            UIO.writeBytes(payload, rawEntryBuffer.bytes, 4 + keyLength + 4);
        }
        UIO.intBytes((int) timestamp, rawEntryBuffer.bytes, 4 + keyLength + 4 + payloadLength);
        return rawEntryBuffer;
    }

    @Override
    public int rawEntryLength(IReadable readable) throws Exception {
        return UIO.readInt(readable, "length");
    }

    @Override
    public void writeRawEntry(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        BolBuffer rawEntry,
        FormatTransformer writeKeyFormatTransormer,
        FormatTransformer writeValueFormatTransormer,
        IAppendOnly appendOnly) throws Exception {
        UIO.writeByteArray(appendOnly, rawEntry.bytes, rawEntry.offset, rawEntry.length, "entry");
    }

    @Override
    public BolBuffer key(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        BolBuffer rawEntry,
        BolBuffer keyBuffer) {
        int length = rawEntry.getInt(0);
        if (length < 0) {
            return null;
        }
        rawEntry.sliceInto(4, length, keyBuffer);
        return keyBuffer;
    }

    @Override
    public ByteBuffer key(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        ByteBuffer rawEntry
    ) {
        rawEntry.clear();
        int keyLength = rawEntry.getInt();
        rawEntry.limit(4 + keyLength);
        return rawEntry.slice();
    }

    @Override
    public int compareKey(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        ByteBuffer rawEntry,
        ByteBuffer compareKey
    ) {
        return IndexUtil.compare(key(readKeyFormatTransormer, readValueFormatTransormer, rawEntry), compareKey);
    }

    @Override
    public int compareKeyFromEntry(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        IReadable readable,
        ByteBuffer compareKey) throws Exception {
        readable.seek(readable.getFilePointer() + 4); // skip the entry length
        int keyLength = readable.readInt();
        return IndexUtil.compare(readable, keyLength, compareKey);
    }

    @Override
    public int compareKeys(ByteBuffer aKey, ByteBuffer bKey) {
        return IndexUtil.compare(aKey, bKey);
    }

    @Override
    public Comparator<ByteBuffer> getByteBufferKeyComparator() {
        return IndexUtil::compare;
    }

    @Override
    public Comparator<byte[]> getKeyComparator() {
        return (byte[] o1, byte[] o2) -> IndexUtil.compare(o1, 0, o1.length, o2, 0, o2.length);
    }

    @Override
    public int compareKey(FormatTransformer aReadKeyFormatTransormer,
        FormatTransformer aReadValueFormatTransormer,
        ByteBuffer aRawEntry,
        FormatTransformer bReadKeyFormatTransormer,
        FormatTransformer bReadValueFormatTransormer,
        ByteBuffer bRawEntry) {

        if (aRawEntry == null && bRawEntry == null) {
            return 0;
        } else if (aRawEntry == null) {
            return -bRawEntry.capacity();
        } else if (bRawEntry == null) {
            return aRawEntry.capacity();
        } else {
            return IndexUtil.compare(
                key(aReadKeyFormatTransormer, aReadValueFormatTransormer, aRawEntry),
                key(bReadKeyFormatTransormer, bReadValueFormatTransormer, bRawEntry)
            );
        }
    }

    @Override
    public boolean mightContain(long timestamp, long timestampVersion, long newerThanTimestamp, long newerThanTimestampVersion) {
        return compareTimestampVersion(timestamp, timestampVersion, newerThanTimestamp, newerThanTimestampVersion) >= 0;
    }

    @Override
    public boolean isNewerThan(long timestamp, long timestampVersion, long newerThanTimestamp, long newerThanTimestampVersion) {
        return compareTimestampVersion(timestamp, timestampVersion, newerThanTimestamp, newerThanTimestampVersion) > 0;
    }

    private static int compareTimestampVersion(long timestamp, long timestampVersion, long otherTimestamp, long otherTimestampVersion) {
        int c = Long.compare(timestamp, otherTimestamp);
        if (c != 0) {
            return c;
        }
        return Long.compare(timestampVersion, otherTimestampVersion);
    }

}
