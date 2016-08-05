package com.jivesoftware.os.miru.plugin.context;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.lab.LABUtils;
import com.jivesoftware.os.lab.api.FormatTransformer;
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
    public byte[] merge(FormatTransformer currentReadKeyFormatTransormer,
        FormatTransformer currentReadValueFormatTransormer,
        byte[] currentRawEntry,
        FormatTransformer addingReadKeyFormatTransormer,
        FormatTransformer addingReadValueFormatTransormer,
        byte[] addingRawEntry,
        FormatTransformer mergedReadKeyFormatTransormer,
        FormatTransformer mergedReadValueFormatTransormer) {
        return addingRawEntry;
    }

    @Override
    public boolean streamRawEntry(int index,
        FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        byte[] rawEntry,
        int offset,
        ValueStream stream,
        boolean hydrateValues) throws Exception {
        if (rawEntry == null) {
            return stream.stream(index, null, -1, false, -1, null);
        }
        int o = offset;
        byte[] key = LABUtils.readByteArray(rawEntry, o);
        o += 4 + (key != null ? key.length : 0);
        byte[] payload = null;
        if (hydrateValues) {
            payload = LABUtils.readByteArray(rawEntry, o);
        }
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
    public int rawEntryLength(IReadable readable) throws Exception {
        return readable.readInt();
    }

    @Override
    public void writeRawEntry(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        byte[] rawEntry,
        int offset,
        int length,
        FormatTransformer writeKeyFormatTransormer,
        FormatTransformer writeValueFormatTransormer,
        IAppendOnly appendOnly) throws Exception {
        UIO.writeByteArray(appendOnly, rawEntry, offset, length, "entry");
    }

    @Override
    public byte[] key(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        byte[] rawEntry,
        int offset,
        int length) {
        return LABUtils.readByteArray(rawEntry, offset);
    }

    @Override
    public int compareKey(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        byte[] rawEntry,
        int offset,
        byte[] compareKey,
        int compareOffset,
        int compareLength) {
        int keylength = UIO.bytesInt(rawEntry, offset);
        return IndexUtil.compare(rawEntry, offset + 4, keylength, compareKey, compareOffset, compareLength);
    }

    @Override
    public int compareKeyFromEntry(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        IReadable readable,
        byte[] compareKey,
        int compareOffset,
        int compareLength) throws Exception {
        readable.seek(readable.getFilePointer() + 4); // skip the entry length
        int keyLength = readable.readInt();
        return IndexUtil.compare(readable, keyLength, compareKey, compareOffset, compareLength);
    }

    @Override
    public int compare(byte[] key1, byte[] key2) {
        return UnsignedBytes.lexicographicalComparator().compare(key1, key2);
    }

    @Override
    public int compareKey(FormatTransformer aReadKeyFormatTransormer,
        FormatTransformer aReadValueFormatTransormer,
        byte[] aRawEntry,
        FormatTransformer bReadKeyFormatTransormer,
        FormatTransformer bReadValueFormatTransormer,
        byte[] bRawEntry) {

        if (aRawEntry == null && bRawEntry == null) {
            return 0;
        } else if (aRawEntry == null) {
            return -bRawEntry.length;
        } else if (bRawEntry == null) {
            return aRawEntry.length;
        } else {
            return compare(
                key(aReadKeyFormatTransormer, aReadValueFormatTransormer, aRawEntry, 0, aRawEntry.length),
                key(bReadKeyFormatTransormer, bReadValueFormatTransormer, bRawEntry, 0, bRawEntry.length)
            );
        }
    }

    @Override
    public long timestamp(FormatTransformer readKeyFormatTransormer, FormatTransformer readValueFormatTransormer, byte[] rawEntry, int offset, int length) {
        return 0;
    }

    @Override
    public long version(FormatTransformer readKeyFormatTransormer, FormatTransformer readValueFormatTransormer, byte[] rawEntry, int offset, int length) {
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
