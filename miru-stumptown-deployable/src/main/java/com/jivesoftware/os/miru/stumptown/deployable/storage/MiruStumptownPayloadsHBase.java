package com.jivesoftware.os.miru.stumptown.deployable.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.TenantRowColumValueTimestampAdd;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MiruStumptownPayloadsHBase implements MiruStumptownPayloadStorage {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();
    private final ObjectMapper objectMapper;
    private final RowColumnValueStore<MiruTenantId, Long, String, byte[], ? extends Exception> payloadTable;

    public MiruStumptownPayloadsHBase(ObjectMapper objectMapper,
        RowColumnValueStore<MiruTenantId, Long, String, byte[], ? extends Exception> payloadTable) {
        this.objectMapper = objectMapper;
        this.payloadTable = payloadTable;
    }

    @Override
    public <T> void multiPut(MiruTenantId tenantId, List<TimeAndPayload<T>> timesAndPayloads) throws Exception {
        List<TenantRowColumValueTimestampAdd<MiruTenantId, Long, String, byte[]>> multiAdd = new ArrayList<>();
        for (TimeAndPayload<T> timeAndPayload : timesAndPayloads) {
            multiAdd.add(new TenantRowColumValueTimestampAdd<>(tenantId,
                timeAndPayload.activityTime, "p", objectMapper.writeValueAsBytes(timeAndPayload.payload), null));
        }
        payloadTable.multiRowsMultiAdd(multiAdd);
    }

    @Override
    public <T> T get(MiruTenantId tenantId, long activityTime, Class<T> payloadClass) throws Exception {
        return objectMapper.readValue(payloadTable.get(tenantId, activityTime, "p", null, null), payloadClass);
    }

    @Override
    public <T> List<T> multiGet(MiruTenantId tenantId, Collection<Long> activityTimes, final Class<T> payloadClass) throws Exception {
        if (activityTimes.isEmpty()) {
            return Collections.emptyList();
        }
        List<Long> rowKeys = new ArrayList<>(activityTimes.size());
        List<String> columnKeys = new ArrayList<>(activityTimes.size());
        for (Long rowKey : activityTimes) {
            rowKeys.add(rowKey);
            columnKeys.add("p");
        }
        List<Map<String, byte[]>> rowColumnValue = payloadTable.multiRowMultiGet(tenantId, rowKeys, columnKeys, null, null);

        List<T> payloads = Lists.newArrayListWithCapacity(rowColumnValue.size());
        for (Map<String, byte[]> columnValue : rowColumnValue) {
            if (columnValue != null && columnValue.containsKey("p")) {
                byte[] bytes = columnValue.get("p");
                if (bytes != null) {
                    try {
                        payloads.add(objectMapper.readValue(bytes, payloadClass));
                    } catch (Exception x) {
                        log.error("Failed mapping " + new String(bytes) + " to " + payloadClass, x);
                    }
                }
            }
        }
        return payloads;
    }


}
