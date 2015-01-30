package com.jivesoftware.os.miru.cluster.rcvs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class MiruActivityPayloads {

    private final ObjectMapper objectMapper;
    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, byte[], ? extends Exception> activityPayloadTable;

    public MiruActivityPayloads(ObjectMapper objectMapper,
        RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, byte[], ? extends Exception> activityPayloadTable) {
        this.objectMapper = objectMapper;
        this.activityPayloadTable = activityPayloadTable;
    }

    public <T> void put(MiruTenantId tenantId, long activityTime, T payload) throws Exception {
        activityPayloadTable.add(MiruVoidByte.INSTANCE, tenantId, activityTime, objectMapper.writeValueAsBytes(payload), null, null);
    }

    public <T> T get(MiruTenantId tenantId, long activityTime, Class<T> payloadClass) throws Exception {
        return objectMapper.readValue(activityPayloadTable.get(MiruVoidByte.INSTANCE, tenantId, activityTime, null, null), payloadClass);
    }

    public <T> List<T> multiGet(MiruTenantId tenantId, Collection<Long> activityTimes, final Class<T> payloadClass) throws Exception {
        Long[] timestamps = activityTimes.toArray(new Long[activityTimes.size()]);
        List<byte[]> payloadBytes = activityPayloadTable.multiGet(MiruVoidByte.INSTANCE, tenantId, timestamps, null, null);
        List<T> payloads = Lists.newArrayListWithCapacity(payloadBytes.size());
        for (byte[] bytes : payloadBytes) {
            payloads.add(objectMapper.readValue(bytes, payloadClass));
        }
        return payloads;
    }

}
