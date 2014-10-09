package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Function;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import java.util.Map;
import javax.annotation.Nullable;

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

    public static <K, V> Function<Map.Entry<K, V>, BulkEntry<K, V>> fromMapEntry() {
        return new Function<Map.Entry<K, V>, BulkEntry<K, V>>() {
            @Nullable
            @Override
            public BulkEntry<K, V> apply(@Nullable Map.Entry<K, V> input) {
                return input != null ? new BulkEntry<>(input.getKey(), input.getValue()) : null;
            }
        };
    }

    public static <K, V> Function<KeyValueStore.Entry<K, V>, BulkEntry<K, V>> fromKeyValueStoreEntry() {
        return new Function<KeyValueStore.Entry<K, V>, BulkEntry<K, V>>() {
            @Nullable
            @Override
            public BulkEntry<K, V> apply(@Nullable KeyValueStore.Entry<K, V> input) {
                return input != null ? new BulkEntry<>(input.getKey(), input.getValue()) : null;
            }
        };
    }
}
