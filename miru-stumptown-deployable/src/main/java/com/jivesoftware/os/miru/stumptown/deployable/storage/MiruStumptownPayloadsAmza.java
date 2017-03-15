package com.jivesoftware.os.miru.stumptown.deployable.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeyStream;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.client.http.HttpPartitionClientFactory;
import com.jivesoftware.os.amza.client.http.HttpPartitionHostsProvider;
import com.jivesoftware.os.amza.client.http.RingHostHttpClientProvider;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.shared.HttpClientException;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MiruStumptownPayloadsAmza implements MiruStumptownPayloadStorage {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ObjectMapper mapper;
    private final AmzaClientProvider<HttpClient, HttpClientException> clientProvider;

    private final PartitionName payload;

    private final PartitionProperties partitionProperties;
    private final long additionalSolverAfterNMillis = 1_000; //TODO expose to conf?
    private final long abandonLeaderSolutionAfterNMillis = 5_000; //TODO expose to conf?
    private final long abandonSolutionAfterNMillis = 30_000; //TODO expose to conf?

    public MiruStumptownPayloadsAmza(String nameSpace,
        ObjectMapper mapper,
        TenantAwareHttpClient<String> httpClient,
        long awaitLeaderElectionForNMillis) {

        this.mapper = mapper;
        BAInterner interner = new BAInterner();

        payload = new PartitionName(false, "p".getBytes(StandardCharsets.UTF_8), (nameSpace + "-stumptown").getBytes(StandardCharsets.UTF_8));

        this.clientProvider = new AmzaClientProvider<>(
            new HttpPartitionClientFactory(interner),
            new HttpPartitionHostsProvider(interner, httpClient, mapper),
            new RingHostHttpClientProvider(httpClient),
            Executors.newCachedThreadPool(), //TODO expose to conf?
            awaitLeaderElectionForNMillis,
            -1,
            -1);

        long ttl = TimeUnit.DAYS.toMillis(1);

        partitionProperties = new PartitionProperties(Durability.fsync_never,
            ttl, ttl / 2, ttl, ttl / 2, ttl, ttl / 2, ttl, ttl / 2,
            false,
            Consistency.leader_quorum,
            true,
            true,
            false,
            RowType.snappy_primary,
            "lab",
            -1,
            null,
            -1,
            -1);
    }

    @Override
    public <T> void multiPut(MiruTenantId tenantId, List<TimeAndPayload<T>> timesAndPayloads) throws Exception {

        PartitionClient partition = clientProvider.getPartition(payload, 3, partitionProperties);
        long now = System.currentTimeMillis();
        partition.commit(Consistency.leader_quorum,
            tenantId.getBytes(), (stream) -> {
                for (TimeAndPayload<T> timeAndPayload : timesAndPayloads) {
                    stream.commit(UIO.longBytes(timeAndPayload.activityTime), mapper.writeValueAsBytes(timeAndPayload.payload), now, false);
                }
                return true;
            },
            additionalSolverAfterNMillis, abandonSolutionAfterNMillis, Optional.empty());

    }

    @Override
    public <T> T get(MiruTenantId tenantId, long activityTime, Class<T> payloadClass) throws Exception {

        PartitionClient partition = clientProvider.getPartition(payload, 3, partitionProperties);
        T[] t = (T[]) new Object[1];
        partition.get(Consistency.leader_quorum,
            tenantId.getBytes(),
            keyStream -> keyStream.stream(UIO.longBytes(activityTime)),
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    t[0] = mapper.readValue(value, payloadClass);
                }
                return false;
            }, additionalSolverAfterNMillis, abandonLeaderSolutionAfterNMillis, abandonSolutionAfterNMillis, Optional.empty());
        return t[0];
    }

    @Override
    public <T> List<T> multiGet(MiruTenantId tenantId, Collection<Long> activityTimes, final Class<T> payloadClass) throws Exception {
        if (activityTimes.isEmpty()) {
            return Collections.emptyList();
        }
        List<T> payloads = Lists.newArrayList();
        PartitionClient partition = clientProvider.getPartition(payload, 3, partitionProperties);
        partition.get(Consistency.leader_quorum,
            tenantId.getBytes(), (UnprefixedWALKeyStream keyStream) -> {
                for (Long activityTime : activityTimes) {
                    if (!keyStream.stream(UIO.longBytes(activityTime))) {
                        return false;
                    }
                }
                return true;
            },
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    payloads.add(mapper.readValue(value, payloadClass));
                }
                return true;
            }, additionalSolverAfterNMillis, abandonLeaderSolutionAfterNMillis, abandonSolutionAfterNMillis, Optional.empty());

        return payloads;
    }

}
