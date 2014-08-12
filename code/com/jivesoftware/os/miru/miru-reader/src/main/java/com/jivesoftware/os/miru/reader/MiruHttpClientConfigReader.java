package com.jivesoftware.os.miru.reader;

import com.google.common.base.Charsets;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.logger.EndPointMetrics;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruConfigReader;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.config.PartitionsForTenantResult;
import java.util.Collection;
import java.util.Map;

public class MiruHttpClientConfigReader implements MiruConfigReader {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final EndPointMetrics processMetrics;
    private final RequestHelper requestHelper;

    public MiruHttpClientConfigReader(RequestHelper requestHelper) {
        this.requestHelper = requestHelper;
        this.processMetrics = new EndPointMetrics("process", LOG);
    }

    @Override
    public Multimap<MiruPartitionState, MiruPartition> getPartitionsForTenant(MiruTenantId tenantId) {
        processMetrics.start();
        try {
            PartitionsForTenantResult partitionsForTenantResult = requestHelper.executeGetRequest(
                CONFIG_SERVICE_ENDPOINT_PREFIX + PARTITIONS_ENDPOINT + "/" + new String(tenantId.getBytes(), Charsets.UTF_8),
                PartitionsForTenantResult.class, PartitionsForTenantResult.DEFAULT_RESULT);
            ArrayListMultimap<MiruPartitionState, MiruPartition> result = ArrayListMultimap.create();
            Map<MiruPartitionState, Collection<MiruPartition>> partitions = partitionsForTenantResult.getPartitions();
            for (Map.Entry<MiruPartitionState, Collection<MiruPartition>> entry : partitions.entrySet()) {
                result.putAll(entry.getKey(), entry.getValue());
            }
            return result;
        } finally {
            processMetrics.stop();
        }
    }
}
