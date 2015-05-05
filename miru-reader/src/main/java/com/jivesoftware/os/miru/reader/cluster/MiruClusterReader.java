package com.jivesoftware.os.miru.reader.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.http.client.HttpClient;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfig;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruConfigReader;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.reader.MiruHttpClientConfigReader;
import com.jivesoftware.os.miru.reader.MiruHttpClientConfigReaderConfig;
import com.jivesoftware.os.miru.reader.MiruHttpClientReader;
import com.jivesoftware.os.miru.reader.MiruHttpClientReaderConfig;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class MiruClusterReader implements MiruReader {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruConfigReader miruConfigReader;
    private final HttpClientFactory httpClientFactory;
    private final ObjectMapper objectMapper;
    private final ExecutorService warmOfflineHostExecutor;
    private final ConcurrentMap<MiruHost, MiruReader> hostToReader = Maps.newConcurrentMap();
    private final Random r = new Random();
    private final boolean userNodeAffinity;

    private final Cache<MiruTenantId, List<MiruPartition>> mostRecentCache;

    public MiruClusterReader(
        MiruHttpClientReaderConfig readerConfig,
        MiruHttpClientConfigReaderConfig configReaderConfig,
        ObjectMapper objectMapper,
        ExecutorService warmOfflineHostExecutor) {

        // MiruConfigReader client configuration
        Collection<HttpClientConfiguration> configReaderConfigurations = Lists.newArrayList();
        HttpClientConfig configurationReaderConfig = HttpClientConfig.newBuilder()
            .setSocketTimeoutInMillis(configReaderConfig.getSocketTimeoutInMillis())
            .setMaxConnections(configReaderConfig.getMaxConnections())
            .build();
        configReaderConfigurations.add(configurationReaderConfig);

        HttpClientFactory configReaderHttpClientFactory = new HttpClientFactoryProvider().createHttpClientFactory(configReaderConfigurations);
        HttpClient httpClient = configReaderHttpClientFactory.createClient(configReaderConfig.getHost(), configReaderConfig.getPort());
        RequestHelper requestHelper = new RequestHelper(httpClient, objectMapper);
        this.miruConfigReader = new MiruHttpClientConfigReader(requestHelper);

        // MiruReader client configuration
        Collection<HttpClientConfiguration> readerConfigurations = Lists.newArrayList();
        HttpClientConfig baseReaderConfig = HttpClientConfig.newBuilder()
            .setSocketTimeoutInMillis(readerConfig.getSocketTimeoutInMillis())
            .setMaxConnections(readerConfig.getMaxConnections())
            .build();
        readerConfigurations.add(baseReaderConfig);

        this.httpClientFactory = new HttpClientFactoryProvider().createHttpClientFactory(readerConfigurations);
        this.warmOfflineHostExecutor = warmOfflineHostExecutor;
        this.objectMapper = objectMapper;
        this.mostRecentCache = CacheBuilder.newBuilder() //TODO config
            .maximumSize(1_000)
            .expireAfterWrite(1, TimeUnit.MINUTES)
            .build();
        this.userNodeAffinity = readerConfig.getUserNodeAffinity();
    }

    @Override
    public void warm(MiruTenantId tenantId) throws MiruQueryServiceException {
        try {
            List<MiruPartition> partitionsForTenant = partitionsForTenant(tenantId);
            submitWarmForOfflineHosts(tenantId, partitionsForTenant);
        } catch (Exception e) {
            mostRecentCache.invalidate(tenantId);
            throw new MiruQueryServiceException("Unable to query for hosts for tenant: " + tenantId, e);
        }
    }

    @Override
    public <P, R> R read(final MiruTenantId tenantId,
        final Optional<MiruActorId> actorId,
        final P params,
        final String endpoint,
        final Class<R> resultClass,
        final R defaultResult)
        throws MiruQueryServiceException {
        try {
            List<MiruPartition> partitionsForTenant = partitionsForTenant(tenantId);
            R result = callForRandomBestOnlineHost(tenantId, partitionsForTenant, defaultResult, randomAffinity(actorId),
                host -> readerForHost(host).read(tenantId, actorId, params, endpoint, resultClass, defaultResult));
            submitWarmForOfflineHosts(tenantId, partitionsForTenant);
            return result;
        } catch (Exception e) {
            mostRecentCache.invalidate(tenantId);
            throw new MiruQueryServiceException("Unable to query for hosts for tenant: " + tenantId, e);
        }
    }

    private Optional<Random> randomAffinity(Optional<MiruActorId> userIdentity) {
        if (userNodeAffinity && userIdentity.isPresent()) {
            return Optional.of(new Random(userIdentity.get().getActorId().hashCode()));
        } else {
            return Optional.absent();
        }
    }

    private <R> R callForRandomBestOnlineHost(MiruTenantId tenantId, List<MiruPartition> partitionsForTenant,
        R defaultResult, Optional<Random> randomAffinity, RandomBestOnlineHostCallback<R> randomBestOnlineHostCallback) {

        for (MiruHost host : randomBestOnlineHost(partitionsForTenant, randomAffinity)) {
            try {
                return randomBestOnlineHostCallback.call(host);
            } catch (MiruQueryServiceException e) {
                log.warn("Failed to call for tenantId:{} using host:{}", new Object[] { tenantId, host }, e);
                mostRecentCache.invalidate(tenantId);
            }
        }
        //TODO bubble an exception instead?
        return defaultResult;
    }

    private void submitWarmForOfflineHosts(final MiruTenantId tenantId, List<MiruPartition> partitions) {
        for (final MiruHost host : offlineHosts(partitions)) {
            try {
                warmOfflineHostExecutor.submit(() -> {
                    try {
                        readerForHost(host).warm(tenantId);
                    } catch (MiruQueryServiceException e) {
                        log.warn("Failed to perform background read for tenantId:{} on host:{}", new Object[] { tenantId, host }, e);
                    }
                });
            } catch (Exception e) {
                log.error("Warm runnable for tenant:{} threw an exception", new Object[] { tenantId }, e);
            }
        }
    }

    private List<MiruHost> randomBestOnlineHost(List<MiruPartition> partitions, Optional<Random> randomAffinity) {
        Set<MiruHost> hosts = Sets.newHashSet();
        for (MiruPartition partition : partitions) {
            if (partition.info.state.equals(MiruPartitionState.online)) {
                hosts.add(partition.coord.host);
            }
        }
        List<MiruHost> truffleShuffle = Lists.newArrayList(hosts);
        Collections.shuffle(truffleShuffle, randomAffinity.or(r));
        return truffleShuffle;
    }

    private Set<MiruHost> offlineHosts(List<MiruPartition> partitions) {
        Set<MiruHost> hosts = Sets.newHashSet();
        for (MiruPartition partition : partitions) {
            if (!partition.info.state.equals(MiruPartitionState.online)) {
                hosts.add(partition.coord.host);
            }
        }
        return hosts;
    }

    private MiruReader readerForHost(MiruHost host) {
        MiruReader reader = hostToReader.get(host);
        if (reader == null) {
            HttpClient httpClient = httpClientFactory.createClient(host.getLogicalName(), host.getPort());
            RequestHelper requestHelper = new RequestHelper(httpClient, objectMapper);
            hostToReader.putIfAbsent(host, new MiruHttpClientReader(requestHelper));
            reader = hostToReader.get(host);
        }
        return reader;
    }

    private List<MiruPartition> partitionsForTenant(MiruTenantId tenantId) throws Exception {
        List<MiruPartition> mostRecentPartitions = mostRecentCache.getIfPresent(tenantId);
        if (mostRecentPartitions == null) {
            List<MiruPartition> partitions = miruConfigReader.getPartitionsForTenant(tenantId);

            // favor the most recent partitions (assume most work can be done locally)
            mostRecentPartitions = extractMostRecentPartitions(partitions);

            boolean hasOneHostOnline = false;
            for (MiruPartition partition : mostRecentPartitions) {
                if (partition.info.state == MiruPartitionState.online) {
                    hasOneHostOnline = true;
                    break;
                }
            }

            if (hasOneHostOnline) {
                mostRecentCache.put(tenantId, mostRecentPartitions);
            }
        }

        return mostRecentPartitions;
    }

    private List<MiruPartition> extractMostRecentPartitions(List<MiruPartition> partitions) {
        MiruPartitionId mostRecentPartition = null;
        for (MiruPartition partition : partitions) {
            if (mostRecentPartition == null || mostRecentPartition.compareTo(partition.coord.partitionId) < 0) {
                mostRecentPartition = partition.coord.partitionId;
            }
        }

        List<MiruPartition> mostRecentPartitions = Lists.newArrayList();
        for (MiruPartition partition : partitions) {
            if (partition.coord.partitionId.equals(mostRecentPartition)) {
                mostRecentPartitions.add(partition);
            }
        }

        return mostRecentPartitions;
    }

    private static interface RandomBestOnlineHostCallback<R> {
        R call(MiruHost host) throws MiruQueryServiceException;
    }

}
