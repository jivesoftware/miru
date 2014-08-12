package com.jivesoftware.os.miru.api.base;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import java.util.Arrays;

/**
 * (IBA) Immutable Byte Array
 *
 * @author jonathan
 */
public class MiruIBA implements Comparable {

    private int hashCode = 0;
    private final byte[] bytes;

    @JsonCreator
    public MiruIBA(@JsonProperty("bytes") byte[] _bytes) {
        bytes = _bytes;
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

        int hash = 0;
        long randMult = 0x5DEECE66DL;
        long randAdd = 0xBL;
        long randMask = (1L << 48) - 1;
        long seed = bytes.length;

        for (int i = 0; i < bytes.length; i++) {
            long x = (seed * randMult + randAdd) & randMask;

            seed = x;
            hash += (bytes[i] + 128) * x;
        }

        hashCode = hash;

        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MiruIBA other = (MiruIBA) obj;
        if (this.hashCode() != other.hashCode()) {
            return false;
        }
        if (!Arrays.equals(this.bytes, other.bytes)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(Object o) {
        byte[] b;
        if (o instanceof byte[]) {
            b = (byte[]) o;
        } else if (o instanceof MiruIBA) {
            b = ((MiruIBA) o).bytes;
        } else {
            b = new byte[0];
        }
        if (b.length < bytes.length) {
            return -1;
        } else if (b.length > bytes.length) {
            return 1;
        } else {
            for (int i = 0; i < bytes.length; i++) {
                if (b[i] < bytes[i]) {
                    return -1;
                } else if (b[i] > bytes[i]) {
                    return 1;
                }
            }
            return 0;
        }
    }
}
