package com.jivesoftware.os.miru.plugin.cache;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import java.nio.ByteBuffer;

/**
 * @author jonathan.colt
 */
public interface MiruPluginCacheProvider {

    CacheKeyValues getKeyValues(String name,
        int payloadSize,
        boolean variablePayloadSize,
        long maxHeapPressureInBytes,
        String hashIndexType,
        double hashIndexLoadFactor);

    interface CacheKeyValues {

        String name();

        boolean get(byte[] cacheId, byte[][] keys, IndexKeyValueStream stream, StackBuffer stackBuffer) throws Exception;

        boolean rangeScan(byte[] cacheId, byte[] fromInclusive, byte[] toExclusive, KeyValueStream stream) throws Exception;

        void put(byte[] cacheId,
            byte[][] keys,
            byte[][] values,
            boolean commitOnUpdate,
            boolean fsyncOnCommit,
            StackBuffer stackBuffer) throws Exception;
    }

    interface IndexKeyValueStream {
        boolean stream(int index, ByteBuffer value) throws Exception;
    }

    interface KeyValueStream {
        boolean stream(ByteBuffer key, ByteBuffer value) throws Exception;
    }

    LastIdCacheKeyValues getLastIdKeyValues(String name,
        int payloadSize,
        boolean variablePayloadSize,
        long maxHeapPressureInBytes,
        String hashIndexType,
        double hashIndexLoadFactor);

    interface LastIdCacheKeyValues {

        String name();

        boolean get(byte[] cacheId, byte[][] keys, LastIdIndexKeyValueStream stream, StackBuffer stackBuffer) throws Exception;

        boolean put(byte[] cacheId,
            boolean commitOnUpdate,
            boolean fsyncOnCommit,
            ConsumeLastIdKeyValueStream consume,
            StackBuffer stackBuffer) throws Exception;
    }

    interface LastIdIndexKeyValueStream {
        boolean stream(int index, ByteBuffer value, int lastId) throws Exception;
    }

    interface LastIdKeyValueStream {
        boolean stream(ByteBuffer key, ByteBuffer value, int lastId) throws Exception;
    }

    interface AppendLastIdKeyValueStream {
        boolean stream(byte[] key, byte[] value, int lastId) throws Exception;
    }

    interface ConsumeLastIdKeyValueStream {
        boolean consume(AppendLastIdKeyValueStream stream) throws Exception;
    }

    TimestampedCacheKeyValues getTimestampedKeyValues(String name,
        int payloadSize,
        boolean variablePayloadSize,
        long maxHeapPressureInBytes,
        String hashIndexType,
        double hashIndexLoadFactor);

    interface TimestampedCacheKeyValues {

        String name();

        boolean get(byte[] cacheId, byte[][] keys, TimestampedIndexKeyValueStream stream, StackBuffer stackBuffer) throws Exception;

        boolean rangeScan(byte[] cacheId, byte[] fromInclusive, byte[] toExclusive, TimestampedKeyValueStream stream) throws Exception;

        boolean put(byte[] cacheId,
            boolean commitOnUpdate,
            boolean fsyncOnCommit,
            ConsumeTimestampedKeyValueStream consume,
            StackBuffer stackBuffer) throws Exception;

        Object lock(byte[] cacheId);
    }

    interface TimestampedIndexKeyValueStream {
        boolean stream(int index, ByteBuffer value, long timestamp) throws Exception;
    }

    interface TimestampedKeyValueStream {
        boolean stream(ByteBuffer key, ByteBuffer value, long timestamp) throws Exception;
    }

    interface AppendTimestampedKeyValueStream {
        boolean stream(byte[] key, byte[] value, long timestamp) throws Exception;
    }

    interface ConsumeTimestampedKeyValueStream {
        boolean consume(AppendTimestampedKeyValueStream stream) throws Exception;
    }
}
