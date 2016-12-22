package com.jivesoftware.os.miru.sync.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.sync.ActivityReadEventConverter;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.NonSuccessStatusCodeException;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall.ClientResponse;
import gnu.trove.impl.Constants;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.hash.TIntIntHashMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 */
public class MiruSyncReceiver<C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruWALClient<C, S> walClient;
    private final TenantAwareHttpClient<String> writerClient;
    private final MiruClusterClient clusterClient;
    private final ActivityReadEventConverter converter;

    private final RoundRobinStrategy roundRobinStrategy = new RoundRobinStrategy();

    public MiruSyncReceiver(MiruWALClient<C, S> walClient,
        TenantAwareHttpClient<String> writerClient,
        MiruClusterClient clusterClient,
        ActivityReadEventConverter converter) {
        this.walClient = walClient;
        this.writerClient = writerClient;
        this.clusterClient = clusterClient;
        this.converter = converter;
    }

    public void writeActivity(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruPartitionedActivity> activities) throws Exception {
        LOG.info("Received from tenantId:{} partitionId:{} activities:{}", tenantId, partitionId, activities.size());
        walClient.writeActivity(tenantId, partitionId, activities);
        updateWriterCursors(tenantId, partitionId, activities);
        if (converter != null) {
            Map<MiruStreamId, List<MiruPartitionedActivity>> converted = converter.convert(tenantId, partitionId, activities);
            if (converted != null) {
                int readCount = 0;
                for (Entry<MiruStreamId, List<MiruPartitionedActivity>> entry : converted.entrySet()) {
                    MiruStreamId streamId = entry.getKey();
                    List<MiruPartitionedActivity> readActivities = entry.getValue();
                    if (!readActivities.isEmpty()) {
                        walClient.writeReadTracking(tenantId, streamId, readActivities);
                        readCount += readActivities.size();
                    }
                }
                LOG.info("Converted from tenantId:{} partitionId:{} readTracks:{}", tenantId, partitionId, readCount);
            }
        }
    }

    private void updateWriterCursors(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruPartitionedActivity> activities) throws Exception {
        List<MiruPartitionedActivity> reversed = Lists.newArrayList(activities);
        Collections.reverse(reversed);
        TIntIntHashMap writerToIndex = new TIntIntHashMap(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, Integer.MIN_VALUE, Integer.MIN_VALUE);
        for (MiruPartitionedActivity activity : reversed) {
            if (activity.type.isActivityType() || activity.type.isBoundaryType()) {
                int existing = writerToIndex.get(activity.writerId);
                if (activity.index > existing) {
                    writerToIndex.put(activity.writerId, activity.index);
                }
            }
        }

        TIntIntIterator iter = writerToIndex.iterator();
        while (iter.hasNext()) {
            iter.advance();
            int writerId = iter.key();
            int index = iter.value();
            String endpoint = "/miru/ingress/cursor/" + writerId + "/" + tenantId + "/" + partitionId + "/" + index;
            writerClient.call("", roundRobinStrategy, "updateWriterCursors", httpClient -> {
                HttpResponse response = httpClient.postJson(endpoint, "{}", null);
                if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                    throw new NonSuccessStatusCodeException(response.getStatusCode(), response.getStatusReasonPhrase());
                }
                return new ClientResponse<Void>(null, true);
            });
            LOG.info("Updated cursor for writer:{} on tenant:{} partition:{} to index:{}", writerId, tenantId, partitionId, index);
        }
    }

    public void registerSchema(MiruTenantId tenantId, MiruSchema schema) throws Exception {
        clusterClient.registerSchema(tenantId, schema);
    }
}
