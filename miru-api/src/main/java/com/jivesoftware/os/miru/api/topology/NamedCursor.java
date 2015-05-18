package com.jivesoftware.os.miru.api.topology;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class NamedCursor implements Comparable<NamedCursor> {

    public final String name;
    public final long id;

    @JsonCreator
    public NamedCursor(@JsonProperty("name") String name,
        @JsonProperty("id") long id) {
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
