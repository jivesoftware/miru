package com.jivesoftware.os.miru.service.index;

/**
 *
 */
public class BulkEntry<K, V> {

    public final K key;
    public final V value;

    public BulkEntry(K key, V value) {
        this.key = key;
        this.value = value;
    }
}
