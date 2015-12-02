package com.jivesoftware.os.miru.api.base;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.primitives.UnsignedBytes;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

/**
 * (IBA) Immutable Byte Array
 *
 * @author jonathan
 */
public class MiruIBA implements Comparable<MiruIBA>, Serializable {

    private static final Comparator<byte[]> LEX_COMPARATOR = UnsignedBytes.lexicographicalComparator();

    private int hashCode = 0;
    private byte[] bytes;

    @JsonCreator
    public MiruIBA(@JsonProperty("bytes") byte[] _bytes) {
        bytes = _bytes;
    }

    public void mutate(byte[] exactBytes, int hashCode) {
        this.bytes = exactBytes;
        this.hashCode = hashCode;
    }

    // this should return a copy to make IBA truly Immutable
    // I have deliberate choosen not to for performance reasons.
    public byte[] immutableBytes() {
        return bytes;
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public String toString() {
        if (bytes == null) {
            return "";
        }
        return new String(bytes, Charsets.UTF_8);
    }

    public int length() {
        return bytes.length;
    }

    @Override
    public int hashCode() {
        if (hashCode != 0) {
            return hashCode;
        }
        if ((bytes == null) || (bytes.length == 0)) {
            return 0;
        }

        int hash = hashCode(bytes, 0, bytes.length);
        hashCode = hash;
        return hash;
    }

    public static int hashCode(byte[] bytes, int offset, int length) {
        int hash = 0;
        long randMult = 0x5_DEEC_E66DL;
        long randAdd = 0xBL;
        long randMask = (1L << 48) - 1;
        long seed = bytes.length;
        for (int i = offset; i < offset + length; i++) {
            long x = (seed * randMult + randAdd) & randMask;

            seed = x;
            hash += (bytes[i] + 128) * x;
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof MiruIBA)) {
            return false;
        }
        final MiruIBA other = (MiruIBA) obj;
        if (this.hashCode() != other.hashCode()) {
            return false;
        }
        return Arrays.equals(this.bytes, other.bytes);
    }

    @Override
    public int compareTo(MiruIBA o) {
        return LEX_COMPARATOR.compare(bytes, o.bytes);
    }

}
