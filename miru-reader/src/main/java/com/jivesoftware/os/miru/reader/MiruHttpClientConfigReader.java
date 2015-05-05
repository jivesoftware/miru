package com.jivesoftware.os.miru.reader;

import com.google.common.base.Charsets;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.MiruConfigReader;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.config.PartitionsForTenantResult;
import com.jivesoftware.os.mlogger.core.EndPointMetrics;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;

public class MiruHttpClientConfigReader implements MiruConfigReader {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final EndPointMetrics processMetrics;
    private final RequestHelper requestHelper;

    public MiruHttpClientConfigReader(RequestHelper requestHelper) {
        this.requestHelper = requestHelper;
        this.processMetrics = new EndPointMetrics("process", LOG);
    }

    @Override
    public List<MiruPartition> getPartitionsForTenant(MiruTenantId tenantId) {
        processMetrics.start();
        try {
            PartitionsForTenantResult partitionsForTenantResult = requestHelper.executeGetRequest(
                CONFIG_SERVICE_ENDPOINT_PREFIX + PARTITIONS_ENDPOINT + "/" + new String(tenantId.getBytes(), Charsets.UTF_8),
                PartitionsForTenantResult.class, PartitionsForTenantResult.DEFAULT_RESULT);
            return partitionsForTenantResult.getPartitions();
        } finally {
            processMetrics.stop();
        }
    }
}
