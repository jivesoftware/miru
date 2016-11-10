package com.jivesoftware.os.wiki.miru.deployable.storage;

/**
 * Created by jonathan.colt on 11/10/16.
 */
public class KeyAndPayload<T> {
    public final String key;
    public final T payload;

    public KeyAndPayload(String key, T payload) {
        this.key = key;
        this.payload = payload;
    }

}
