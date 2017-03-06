package com.jivesoftware.os.miru.plugin.solution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import java.io.Serializable;
import java.util.Arrays;

/**
 *
 */
public class Waveform implements Serializable {

    private static final byte[] EMPTY = new byte[0];

    public static Waveform empty(MiruValue id, int length) {
        return new Waveform(id, length, EMPTY);
    }

    public static Waveform compressed(MiruValue id, long[] rawWaveform) {
        return new Waveform(id, -1, null).compress(rawWaveform);
    }

    private MiruValue id;
    private int offset;
    private byte[] waveform;

    private Waveform(MiruValue id, int offset, byte[] waveform) {
        this.id = id;
        this.offset = offset;
        this.waveform = waveform;
    }

    @JsonCreator
    public static Waveform fromJson(@JsonProperty("id") MiruValue id,
        @JsonProperty("offset") int offset,
        @JsonProperty("waveform") byte[] waveform) throws Exception {
        return new Waveform(id, offset, waveform);
    }

    @JsonGetter("id")
    public MiruValue getId() {
        return id;
    }

    @JsonGetter("offset")
    public int getRawOffset() {
        return offset;
    }

    @JsonGetter("waveform")
    public byte[] getRawBytes() {
        return waveform;
    }

    @JsonIgnore
    public Waveform compress(long[] rawWaveform) {
        int offset = rawWaveform.length;

        for (int i = 0; i < rawWaveform.length; i++) {
            if (rawWaveform[i] != 0) {
                offset = i;
                break;
            }
        }
        int count = rawWaveform.length - offset;
        for (int i = rawWaveform.length - 1; i >= offset; i--) {
            if (rawWaveform[i] != 0) {
                count = (i + 1 - offset);
                break;
            }
        }

        int precision = 0;
        for (int i = offset; i < offset + count; i++) {
            precision = Math.max(precision(rawWaveform[i]), precision);
        }

        this.offset = offset;
        this.waveform = longsBytes(rawWaveform, offset, count, (byte) precision);
        return this;
    }

    @JsonIgnore
    public void mergeWaveform(long[] rawWaveform) {
        mergeBytesLongs(waveform, rawWaveform, offset);
    }

    @Override
    public String toString() {
        if (waveform.length > 0) {
            byte precision = waveform[waveform.length - 1];
            long[] longs = precision > 0 ? new long[(waveform.length - 1) / precision] : new long[0];
            mergeBytesLongs(waveform, longs, 0);
            return "Waveform{"
                + "offset=" + offset
                + ", waveform=" + Arrays.toString(longs)
                + '}';
        } else {
            return "";
        }
    }

    static int precision(long value) {
        if (value == 0) {
            return 0;
        }
        if (value < 0) {
            value = Math.abs(value + 1);
        }
        int numberOfTrailingZeros = Long.numberOfLeadingZeros(value);
        return 1 + (64 - numberOfTrailingZeros) / 8;
    }

    static byte[] longsBytes(long[] _longs, int offset, int count, byte precision) {
        byte[] bytes = new byte[count * precision + 1];
        for (int i = 0; i < count; i++) {
            longBytes(_longs[offset + i], bytes, i * precision, precision);
        }
        bytes[bytes.length - 1] = precision;
        return bytes;
    }

    static byte[] longBytes(long v, byte[] _bytes, int _offset, byte precision) {
        int offset = _offset;
        if (precision > 7) {
            _bytes[offset++] = (byte) (v >>> 56);
        }
        if (precision > 6) {
            _bytes[offset++] = (byte) (v >>> 48);
        }
        if (precision > 5) {
            _bytes[offset++] = (byte) (v >>> 40);
        }
        if (precision > 4) {
            _bytes[offset++] = (byte) (v >>> 32);
        }
        if (precision > 3) {
            _bytes[offset++] = (byte) (v >>> 24);
        }
        if (precision > 2) {
            _bytes[offset++] = (byte) (v >>> 16);
        }
        if (precision > 1) {
            _bytes[offset++] = (byte) (v >>> 8);
        }
        if (precision > 0) {
            _bytes[offset++] = (byte) v;
        }
        return _bytes;
    }

    static void mergeBytesLongs(byte[] _bytes, long[] longs, int offset) {
        if (_bytes == null || _bytes.length == 0) {
            return;
        }
        byte precision = _bytes[_bytes.length - 1];
        if (precision > 0) {
            int longsCount = (_bytes.length - 1) / precision;
            for (int i = 0; i < longsCount; i++) {
                longs[offset + i] += bytesLong(_bytes, i * precision, precision);
            }
        }
    }

    static long bytesLong(byte[] bytes, int _offset, byte precision) {
        if (bytes == null) {
            return 0;
        }
        int offset = _offset;
        long v = 0;
        if (precision > 7) {
            v |= (bytes[offset++] & 0xFF);
            v <<= 8;
        }
        if (precision > 6) {
            v |= (bytes[offset++] & 0xFF);
            v <<= 8;
        }
        if (precision > 5) {
            v |= (bytes[offset++] & 0xFF);
            v <<= 8;
        }
        if (precision > 4) {
            v |= (bytes[offset++] & 0xFF);
            v <<= 8;
        }
        if (precision > 3) {
            v |= (bytes[offset++] & 0xFF);
            v <<= 8;
        }
        if (precision > 2) {
            v |= (bytes[offset++] & 0xFF);
            v <<= 8;
        }
        if (precision > 1) {
            v |= (bytes[offset++] & 0xFF);
            v <<= 8;
        }
        if (precision > 0) {
            v |= (bytes[offset++] & 0xFF);

            if (precision < 8 && (bytes[_offset] & 0x80) != 0) {
                v |= ((~0L) << (8 * precision));
            }
        }
        return v;
    }

}
