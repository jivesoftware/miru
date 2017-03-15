package com.jivesoftware.os.miru.plugin.context;

import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.api.rawhide.Rawhide;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IPointerReadable;
import com.jivesoftware.os.lab.io.api.UIO;

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
    public int mergeCompare(FormatTransformer aReadKeyFormatTransormer,
        FormatTransformer aReadValueFormatTransormer,
        BolBuffer aRawEntry,
        BolBuffer aKeyBuffer,
        FormatTransformer bReadKeyFormatTransormer,
        FormatTransformer bReadValueFormatTransormer,
        BolBuffer bRawEntry,
        BolBuffer bKeyBuffer) throws Exception {

        int c = compareKey(aReadKeyFormatTransormer, aReadValueFormatTransormer, aRawEntry, aKeyBuffer,
            bReadKeyFormatTransormer, bReadValueFormatTransormer, bRawEntry, bKeyBuffer);
        if (c != 0) {
            return c;
        }

        if (aRawEntry == null && bRawEntry == null) {
            return 0;
        } else if (aRawEntry == null) {
            return -bRawEntry.length;
        } else if (bRawEntry == null) {
            return aRawEntry.length;
        } else {

            long asTimestamp = (long) aRawEntry.getInt(aRawEntry.length - 4);
            long bsTimestamp = (long) bRawEntry.getInt(bRawEntry.length - 4);

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
    public boolean hasTimestampVersion() {
        return true;
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
        BolBuffer rawEntry,
        BolBuffer keyBuffer,
        BolBuffer valueBuffer,
        ValueStream stream) throws Exception {
        if (rawEntry == null) {
            return stream.stream(index, null, -1, false, -1, null);
        }

        int keyLength = rawEntry.getInt(0);
        BolBuffer key = rawEntry.sliceInto(4, keyLength, keyBuffer);

        int payloadLength = rawEntry.getInt(4 + keyLength);
        int lastId = rawEntry.getInt(4 + keyLength + 4 + payloadLength - 4);

        BolBuffer payload = null;
        if (valueBuffer != null) {
            payload = rawEntry.sliceInto(4 + keyLength + 4, payloadLength - 4, valueBuffer);
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
    public int rawEntryToBuffer(IPointerReadable readable, long offset, BolBuffer entryBuffer) throws Exception {
        int length = readable.readInt(offset);
        readable.sliceIntoBuffer(offset + 4, length, entryBuffer);
        return 4 + length;
    }

    @Override
    public void writeRawEntry(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        BolBuffer rawEntryBuffer,
        FormatTransformer writeKeyFormatTransormer,
        FormatTransformer writeValueFormatTransormer,
        IAppendOnly appendOnly) throws Exception {
        appendOnly.appendInt(rawEntryBuffer.length);
        appendOnly.append(rawEntryBuffer);
    }

    @Override
    public BolBuffer key(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        BolBuffer rawEntry,
        BolBuffer keyBuffer) {
        return rawEntry.sliceInto(4, rawEntry.getInt(0), keyBuffer);
    }

}
