package com.jivesoftware.os.miru.plugin.context;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import java.io.IOException;

/**
 * @author jonathan.colt
 */
public interface MiruPluginCacheProvider {

    CacheKeyValues get(String name, int payloadSize, boolean variablePayloadSize);

    interface CacheKeyValues {

        boolean get(String cacheId, byte[][] keys, GetKeyValueStream stream, StackBuffer stackBuffer) throws Exception;

        void put(String cacheId, byte[][] keys, byte[][] values, StackBuffer stackBuffer) throws Exception;
    }

    interface GetKeyValueStream {
        boolean stream(int index, byte[] key, byte[] value) throws IOException;
    }
}
