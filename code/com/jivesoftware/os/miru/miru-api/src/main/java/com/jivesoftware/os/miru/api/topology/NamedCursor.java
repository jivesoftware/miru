package com.jivesoftware.os.miru.api.topology;

/**
 *
 */
public class NamedCursor {

    public String name;
    public long id;

    public NamedCursor() {
    }

    public NamedCursor(String name, long id) {
        this.name = name;
        this.id = id;
    }
}
