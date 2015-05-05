package com.jivesoftware.os.miru.api.marshall;

/**
 *
 */
public class MiruVoidByte implements Comparable<MiruVoidByte> {

    public static final MiruVoidByte INSTANCE = new MiruVoidByte();

    private MiruVoidByte() {
    }

    @Override
    public String toString() {
        return "";
    }

    @Override
    public int compareTo(MiruVoidByte o) {
        return 0;
    }
}
