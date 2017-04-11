package com.jivesoftware.os.miru.catwalk.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery.CatwalkDefinition;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery.CatwalkFeature;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery.CatwalkModelQuery;
import com.jivesoftware.os.miru.catwalk.shared.Strategy;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 */
public class CatwalkModelQueueTest {
    @Test
    public void testKeyValueSerDer() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        MiruTenantId tenantId = new MiruTenantId("tenantId".getBytes());
        String catwalkId = "catwalkId";
        String modelId = "modelId";
        int partitionId = 1234;
        CatwalkQuery catwalkQuery = new CatwalkQuery(
            new CatwalkDefinition(catwalkId,
                "scorableField",
                new CatwalkFeature[] {
                    new CatwalkFeature("feature1", new String[] { "abc", "def" }, MiruFilter.NO_FILTER, 1f)
                },
                Strategy.UNIT_WEIGHTED,
                MiruFilter.NO_FILTER,
                1),
            new CatwalkModelQuery(MiruTimeRange.ALL_TIME,
                new MiruFilter[] { MiruFilter.NO_FILTER },
                100));
        long timestamp = System.currentTimeMillis();

        byte[] keyBytes = CatwalkModelQueue.updateModelKey(tenantId, catwalkId, modelId, partitionId);
        byte[] valueBytes = mapper.writeValueAsBytes(catwalkQuery);

        UpdateModelRequest updateModelRequest = CatwalkModelQueue.updateModelRequestFromBytes(mapper, keyBytes, valueBytes, timestamp);

        assertEquals(updateModelRequest.tenantId, tenantId);
        assertEquals(updateModelRequest.catwalkId, catwalkId);
        assertEquals(updateModelRequest.modelId, modelId);
        assertEquals(updateModelRequest.partitionId, partitionId);
        assertEquals(updateModelRequest.catwalkQuery.toString(), catwalkQuery.toString()); // LOL
        assertEquals(updateModelRequest.timestamp, timestamp);
    }
}
