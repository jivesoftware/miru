package com.jivesoftware.os.wiki.miru.deployable.storage;

import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public interface WikiMiruPayloadStorage {

    <T> T get(MiruTenantId tenantId, long activityTime, Class<T> payloadClass) throws Exception;

    <T> List<T> multiGet(MiruTenantId tenantId, Collection<Long> activityTimes, final Class<T> payloadClass) throws Exception;

    <T> void multiPut(MiruTenantId tenantId, List<TimeAndPayload<T>> timesAndPayloads) throws Exception;

    public static class TimeAndPayload<T> {

        public final long activityTime;
        public final T payload;

        public TimeAndPayload(long activityTime, T payload) {
            this.activityTime = activityTime;
            this.payload = payload;
        }
    }

}
