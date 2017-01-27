package com.jivesoftware.os.miru.plugin.index;

/**
 *
 */
public class ValueBitsIndex {

    public static byte[] shortBytes(short v, byte[] bytes, int offset) {
        bytes[offset + 0] = (byte) (v >>> 8);
        bytes[offset + 1] = (byte) v;
        return bytes;
    }

    public static short bytesShort(byte[] bytes) {
        return bytesShort(bytes, 0);
    }

    public static short bytesShort(byte[] bytes, int offset) {
        short v = 0;
        v |= (bytes[offset + 0] & 0xFF);
        v <<= 8;
        v |= (bytes[offset + 1] & 0xFF);
        return v;
    }

    public static byte[] unsignedShortBytes(int length, byte[] bytes, int offset) {
        bytes[offset + 0] = (byte) (length >>> 8);
        bytes[offset + 1] = (byte) length;
        return bytes;
    }

    public static int bytesUnsignedShort(byte[] bytes, int offset) {
        int v = 0;
        v |= (bytes[offset + 0] & 0xFF);
        v <<= 8;
        v |= (bytes[offset + 1] & 0xFF);
        return v;
    }

    public static byte[] packValue(byte[] raw) {
        int length = raw.length;
        byte[] packed = new byte[2 + raw.length];
        System.arraycopy(raw, 0, packed, 2, raw.length);
        unsignedShortBytes(length, packed, 0);
        return packed;
    }

    public static byte[] unpackValue(byte[] packed) {
        int length = bytesUnsignedShort(packed, 0);
        if (length == 0) {
            return null;
        }
        byte[] raw = new byte[length];
        System.arraycopy(packed, 2, raw, 0, length);
        return raw;
    }
}
