package com.jivesoftware.os.miru.plugin.cache;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import java.io.IOException;

/**
 * @author jonathan.colt
 */
public interface MiruPluginCacheProvider {

    CacheKeyValues get(String name, int payloadSize, boolean variablePayloadSize, int maxUpdatesBeforeFlush);

    interface CacheKeyValues {

        boolean get(byte[] cacheId, byte[][] keys, GetKeyValueStream stream, StackBuffer stackBuffer) throws Exception;

        boolean rangeScan(byte[] cacheId, byte[] fromInclusive, byte[] toExclusive, RangeKeyValueStream stream) throws Exception;

        void put(byte[] cacheId,
            byte[][] keys,
            byte[][] values,
            boolean commitOnUpdate,
            boolean fsyncOnCommit,
            StackBuffer stackBuffer) throws Exception;
    }

    interface GetKeyValueStream {
        boolean stream(int index, byte[] key, byte[] value) throws IOException;
    }

    interface RangeKeyValueStream {
        boolean stream(byte[] key, byte[] value) throws IOException;
    }
}
