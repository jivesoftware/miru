package com.jivesoftware.os.wiki.miru.deployable.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.client.http.HttpPartitionClientFactory;
import com.jivesoftware.os.amza.client.http.HttpPartitionHostsProvider;
import com.jivesoftware.os.amza.client.http.RingHostHttpClientProvider;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.TailAtScaleStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.HttpClientException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class WikiMiruPayloadsAmza {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ObjectMapper mapper;
    private final AmzaClientProvider<HttpClient, HttpClientException> clientProvider;

    private final String nameSpace;
    private final PartitionProperties partitionProperties;
    private final long additionalSolverAfterNMillis = 1_000; //TODO expose to conf?
    private final long abandonLeaderSolutionAfterNMillis = 5_000; //TODO expose to conf?
    private final long abandonSolutionAfterNMillis = 30_000; //TODO expose to conf?

    public WikiMiruPayloadsAmza(String nameSpace,
        ObjectMapper mapper,
        TenantAwareHttpClient<String> httpClient,
        long awaitLeaderElectionForNMillis) {

        this.nameSpace = nameSpace;
        this.mapper = mapper;

        TailAtScaleStrategy tailAtScaleStrategy = new TailAtScaleStrategy(
            new ThreadPoolExecutor(0, 1024,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadFactoryBuilder().setNameFormat("tas-%d").build()),
            100, // TODO config
            95, // TODO config
            1000
        );


        this.clientProvider = new AmzaClientProvider<>(
            new HttpPartitionClientFactory(),
            new HttpPartitionHostsProvider(httpClient, tailAtScaleStrategy, mapper),
            new RingHostHttpClientProvider(httpClient),
            Executors.newCachedThreadPool(), //TODO expose to conf?
            awaitLeaderElectionForNMillis,
            -1,
            -1);

        partitionProperties = new PartitionProperties(Durability.fsync_async,
            -1, -1, -1, -1, -1, -1, -1, -1,
            false,
            Consistency.none,
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

    private PartitionName getPartitionName(MiruTenantId tenantId) {
        byte[] pname = (nameSpace + "-" + tenantId.toString() + "-wiki").getBytes(StandardCharsets.UTF_8);
        return new PartitionName(false, pname, pname);
    }

    public <T> void multiPut(MiruTenantId tenantId, List<KeyAndPayload<T>> timesAndPayloads) throws Exception {

        PartitionClient partition = clientProvider.getPartition(getPartitionName(tenantId), 3, partitionProperties);
        long now = System.currentTimeMillis();
        partition.commit(Consistency.quorum,
            null,
            (stream) -> {
                for (KeyAndPayload<T> keyAndPayload : timesAndPayloads) {
                    stream.commit(keyAndPayload.key.getBytes(StandardCharsets.UTF_8), mapper.writeValueAsBytes(keyAndPayload.payload), now, false);
                }
                return true;
            },
            additionalSolverAfterNMillis, abandonSolutionAfterNMillis, Optional.empty());

    }

    public <T> T get(MiruTenantId tenantId, String k, Class<T> payloadClass) throws Exception {

        PartitionClient partition = clientProvider.getPartition(getPartitionName(tenantId), 3, partitionProperties);
        T[] t = (T[]) new Object[1];
        partition.get(Consistency.none,
            null,
            keyStream -> keyStream.stream(k.getBytes(StandardCharsets.UTF_8)),
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    t[0] = mapper.readValue(value, payloadClass);
                }
                return false;
            }, additionalSolverAfterNMillis, abandonLeaderSolutionAfterNMillis, abandonSolutionAfterNMillis, Optional.empty());
        return t[0];
    }

    public <T> List<T> multiGet(MiruTenantId tenantId, Collection<String> keys, final Class<T> payloadClass) throws Exception {
        if (keys.isEmpty()) {
            return Collections.emptyList();
        }
        List<T> payloads = Lists.newArrayList();
        PartitionClient partition = clientProvider.getPartition(getPartitionName(tenantId), 3, partitionProperties);
        partition.get(Consistency.none,
            null,
            (keyStream) -> {
                for (String key : keys) {
                    if (!keyStream.stream(key.getBytes(StandardCharsets.UTF_8))) {
                        return false;
                    }
                }
                return true;
            },
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    payloads.add(mapper.readValue(value, payloadClass));
                } else {
                    payloads.add(null);
                }
                return true;
            }, additionalSolverAfterNMillis, abandonLeaderSolutionAfterNMillis, abandonSolutionAfterNMillis, Optional.empty());

        return payloads;
    }

}
