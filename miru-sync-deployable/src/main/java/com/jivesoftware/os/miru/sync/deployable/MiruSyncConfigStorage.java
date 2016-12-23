package com.jivesoftware.os.miru.sync.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.BAInterner;
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
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.Executors;

/**
 *
 */
public class MiruSyncConfigStorage {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ObjectMapper mapper;
    private final AmzaClientProvider<HttpClient, HttpClientException> clientProvider;

    private final PartitionName config;

    private final PartitionProperties partitionProperties;
    private final long additionalSolverAfterNMillis = 1_000; //TODO expose to conf?
    private final long abandonLeaderSolutionAfterNMillis = 5_000; //TODO expose to conf?
    private final long abandonSolutionAfterNMillis = 30_000; //TODO expose to conf?

    public MiruSyncConfigStorage(String nameSpace,
        ObjectMapper mapper,
        TenantAwareHttpClient<String> httpClient,
        long awaitLeaderElectionForNMillis) {

        this.mapper = mapper;
        BAInterner interner = new BAInterner();

        byte[] nameAsBytes = (nameSpace + "-miru-sync-config").getBytes(StandardCharsets.UTF_8);
        config = new PartitionName(false, nameAsBytes, nameAsBytes);

        this.clientProvider = new AmzaClientProvider<>(
            new HttpPartitionClientFactory(interner),
            new HttpPartitionHostsProvider(interner, httpClient, mapper),
            new RingHostHttpClientProvider(httpClient),
            Executors.newCachedThreadPool(), //TODO expose to conf?
            awaitLeaderElectionForNMillis,
            -1,
            -1);

        partitionProperties = new PartitionProperties(Durability.fsync_async,
            0, 0, 0, 0, 0, 0, 0, 0,
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

    public void multiPutIfAbsent(Map<MiruTenantId, MiruSyncTenantConfig> whitelistTenantIds) throws Exception {
        Map<MiruTenantId, MiruSyncTenantConfig> got = multiGet(whitelistTenantIds.keySet());
        Map<MiruTenantId, MiruSyncTenantConfig> put = Maps.newHashMap();
        for (Entry<MiruTenantId, MiruSyncTenantConfig> entry : whitelistTenantIds.entrySet()) {
            if (!got.containsKey(entry.getKey())) {
                put.put(entry.getKey(), entry.getValue());
            }
        }
        if (!put.isEmpty()) {
            multiPut(put);
        }
    }

    public void multiPut(Map<MiruTenantId, MiruSyncTenantConfig> configs) throws Exception {

        PartitionClient partition = clientProvider.getPartition(config, 3, partitionProperties);
        long now = System.currentTimeMillis();
        partition.commit(Consistency.leader_quorum,
            null,
            (stream) -> {
                for (Entry<MiruTenantId, MiruSyncTenantConfig> configEntry : configs.entrySet()) {
                    stream.commit(configEntry.getKey().getBytes(), mapper.writeValueAsBytes(configEntry.getValue()), now, false);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());

        LOG.info("Put {} configs.", configs.size());

    }

    public void multiRemove(List<MiruTenantId> tenantIds) throws Exception {

        PartitionClient partition = clientProvider.getPartition(config, 3, partitionProperties);
        long now = System.currentTimeMillis();
        partition.commit(Consistency.leader_quorum,
            null,
            (stream) -> {
                for (MiruTenantId tenantId : tenantIds) {
                    stream.commit(tenantId.getBytes(), null, now, true);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());

        LOG.info("Removed {} configs.", tenantIds.size());

    }

    public Map<MiruTenantId, MiruSyncTenantConfig> multiGet(Collection<MiruTenantId> tenantIds) throws Exception {
        if (tenantIds.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<MiruTenantId, MiruSyncTenantConfig> got = Maps.newConcurrentMap();
        PartitionClient partition = clientProvider.getPartition(config, 3, partitionProperties);
        partition.get(Consistency.leader_quorum,
            null,
            (keyStream) -> {
                for (MiruTenantId tenantId : tenantIds) {
                    if (!keyStream.stream(tenantId.getBytes())) {
                        return false;
                    }
                }
                return true;
            },
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    got.put(new MiruTenantId(key), mapper.readValue(value, MiruSyncTenantConfig.class));
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty()
        );

        LOG.info("Got {} configs.", got.size());

        return got;
    }

    public <T> Map<MiruTenantId, MiruSyncTenantConfig> getAll() throws Exception {
        Map<MiruTenantId, MiruSyncTenantConfig> got = Maps.newConcurrentMap();
        PartitionClient partition = clientProvider.getPartition(config, 3, partitionProperties);
        partition.scan(Consistency.leader_quorum,
            false,
            null,
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    got.put(new MiruTenantId(key), mapper.readValue(value, MiruSyncTenantConfig.class));
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty()
        );

        LOG.info("Got All {} configs.", got.size());
        return got;
    }


}
