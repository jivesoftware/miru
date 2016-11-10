package com.jivesoftware.os.wiki.miru.deployable.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Multiset;
import com.google.common.primitives.Bytes;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.filer.UIO;
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
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class WikiMiruGramsAmza {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ObjectMapper mapper;
    private final AmzaClientProvider<HttpClient, HttpClientException> clientProvider;

    private final String nameSpace;
    private final PartitionProperties partitionProperties;
    private final long additionalSolverAfterNMillis = 1_000; //TODO expose to conf?
    private final long abandonLeaderSolutionAfterNMillis = 5_000; //TODO expose to conf?
    private final long abandonSolutionAfterNMillis = 30_000; //TODO expose to conf?

    public WikiMiruGramsAmza(String nameSpace,
        ObjectMapper mapper,
        TenantAwareHttpClient<String> httpClient,
        long awaitLeaderElectionForNMillis) {

        this.nameSpace = nameSpace;
        this.mapper = mapper;
        BAInterner interner = new BAInterner();

        this.clientProvider = new AmzaClientProvider<>(
            new HttpPartitionClientFactory(interner),
            new HttpPartitionHostsProvider(interner, httpClient, mapper),
            new RingHostHttpClientProvider(httpClient),
            Executors.newCachedThreadPool(), //TODO expose to conf?
            awaitLeaderElectionForNMillis,
            -1,
            -1);

        long tombstoneTimestampAgeInMillis = TimeUnit.HOURS.toMillis(1);

        partitionProperties = new PartitionProperties(Durability.fsync_async,
            tombstoneTimestampAgeInMillis,tombstoneTimestampAgeInMillis/2, tombstoneTimestampAgeInMillis, tombstoneTimestampAgeInMillis/2, -1, -1, -1, -1,
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

    private PartitionName getPartitionName(MiruTenantId tenantId) {
        byte[] pname = (nameSpace + "-" + tenantId.toString() + "-grams").getBytes(StandardCharsets.UTF_8);
        return new PartitionName(false, pname, pname);
    }

    public <T> void multiPut(MiruTenantId tenantId, Multiset<String> grams) throws Exception {

        PartitionClient partition = clientProvider.getPartition(getPartitionName(tenantId), 3, partitionProperties);
        long now = System.currentTimeMillis();
        partition.commit(Consistency.leader_quorum,
            null,
            (stream) -> {
                for (String gram : grams) {
                    byte[] gramBytes = gram.getBytes(StandardCharsets.UTF_8);
                    byte[] key = Bytes.concat(UIO.intBytes(gramBytes.length), gramBytes, UIO.longBytes(System.currentTimeMillis()));
                    stream.commit(key, UIO.intBytes(grams.count(gram)), now, false);
                }
                return true;
            },
            additionalSolverAfterNMillis, abandonSolutionAfterNMillis, Optional.empty());

    }

    /*public <T> T get(MiruTenantId tenantId, String k, Class<T> payloadClass) throws Exception {

        PartitionClient partition = clientProvider.getPartition(getPartitionName(tenantId), 3, partitionProperties);
        T[] t = (T[]) new Object[1];
        partition.get(Consistency.leader_quorum,
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
        partition.get(Consistency.leader_quorum,
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
                }
                return true;
            }, additionalSolverAfterNMillis, abandonLeaderSolutionAfterNMillis, abandonSolutionAfterNMillis, Optional.empty());

        return payloads;
    }*/

}
