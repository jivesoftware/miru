package com.jivesoftware.os.miru.cluster.rcvs;

/**
*
*/
public class MiruSchemaColumnKey implements Comparable<MiruSchemaColumnKey> {

    public final String name;
    public final long version;

    public MiruSchemaColumnKey(String name, long version) {
        this.name = name;
        this.version = version;
    }

    @Override
    public int compareTo(MiruSchemaColumnKey o) {
        int c = -Long.compare(version, o.version); // reverse version order
        if (c == 0) {
            c = name.compareTo(o.name);
        }
        return c;
    }
}
