package com.jivesoftware.os.wiki.miru.deployable.storage;

import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public interface WikiMiruPayloadStorage {

    <T> T get(MiruTenantId tenantId, String key, Class<T> payloadClass) throws Exception;

    <T> List<T> multiGet(MiruTenantId tenantId, Collection<String> keys, final Class<T> payloadClass) throws Exception;

    <T> void multiPut(MiruTenantId tenantId, List<KeyAndPayload<T>> timesAndPayloads) throws Exception;

    public static class KeyAndPayload<T> {

        public final String key;
        public final T payload;

        public KeyAndPayload(String key, T payload) {
            this.key = key;
            this.payload = payload;
        }
    }

}
