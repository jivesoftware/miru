package com.jivesoftware.os.miru.api.topology;

/**
 *
 */
public class NamedCursor implements Comparable<NamedCursor> {

    public String name;
    public long id;

    public NamedCursor() {
    }

    public NamedCursor(String name, long id) {
        this.name = name;
        this.id = id;
    }

    @Override
    public int compareTo(NamedCursor o) {
        int c = Long.compare(o.id, id); // reversed for descending order
        if (c == 0) {
            c = o.name.compareTo(name);
        }
        return c;
    }

    @Override
    public String toString() {
        return "NamedCursor{" +
            "name='" + name + '\'' +
            ", id=" + id +
            '}';
    }
}
