package com.jivesoftware.os.miru.wal.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import com.jivesoftware.os.miru.api.activity.StreamIdPartitionedActivities;
import com.jivesoftware.os.miru.api.activity.TimeAndVersion;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.AmzaCursor;
import com.jivesoftware.os.miru.api.wal.AmzaSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruVersionedActivityLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.miru.api.wal.SipAndLastSeen;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.health.checkers.SickThreads;
import com.jivesoftware.os.routing.bird.http.client.ConnectionDescriptorSelectiveStrategy;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.TailAtScaleStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.routing.bird.shared.ClientCall.ClientResponse;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import com.jivesoftware.os.routing.bird.shared.HttpClientException;
import com.jivesoftware.os.routing.bird.shared.NextClientStrategy;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.http.HttpStatus;

public class AmzaHttpWALClient implements MiruWALClient<AmzaCursor, AmzaSipCursor> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String routingTenantId;
    private final TenantAwareHttpClient<String> walClient;
    private final ExecutorService tasExecutors;
    private final int tasWindowSize;
    private final float tasPercentile;
    private final long tasInitialSLAMillis;
    private final ObjectMapper requestMapper;
    private final HttpResponseMapper responseMapper;
    private final SickThreads sickThreads;
    private final long sleepOnFailureMillis;
    private final Cache<TenantRoutingGroup<?>, NextClientStrategy> tenantRoutingCache;

    private final Map<MiruTenantId, NextClientStrategy> tenantNextClientStrategy = Maps.newConcurrentMap();

    public AmzaHttpWALClient(String routingTenantId,
        TenantAwareHttpClient<String> walClient,
        ExecutorService tasExecutors,
        int tasWindowSize,
        float tasPercentile,
        long tasInitialSLAMillis,
        ObjectMapper requestMapper,
        HttpResponseMapper responseMapper,
        SickThreads sickThreads, long sleepOnFailureMillis) {
        this.routingTenantId = routingTenantId;
        this.walClient = walClient;
        this.tasExecutors = tasExecutors;
        this.tasWindowSize = tasWindowSize;
        this.tasPercentile = tasPercentile;
        this.tasInitialSLAMillis = tasInitialSLAMillis;
        this.requestMapper = requestMapper;
        this.responseMapper = responseMapper;
        this.sickThreads = sickThreads;
        this.sleepOnFailureMillis = sleepOnFailureMillis;
        this.tenantRoutingCache = CacheBuilder.newBuilder()
            .maximumSize(50_000) //TODO config
            .expireAfterWrite(5, TimeUnit.MINUTES) //TODO config
            .build();
    }

    private HostPort[] getTenantRoutingGroup(RoutingGroupType routingGroupType, MiruTenantId tenantId, boolean createIfAbsent) {
        return send(tenantId, "getTenantRoutingGroup", httpClient -> {
            HttpResponse httpResponse = httpClient.get(
                "/miru/wal/amza/routing/lazyTenant" +
                    "/" + routingGroupType.name() +
                    "/" + tenantId.toString() +
                    "/" + createIfAbsent,
                null);
            HostPort[] response = responseMapper.extractResultFromResponse(httpResponse, HostPort[].class, null);
            return new ClientResponse<>(response, true);
        });
    }

    private HostPort[] getTenantPartitionRoutingGroup(RoutingGroupType routingGroupType,
        MiruTenantId tenantId,
        MiruPartitionId partitionId,
        boolean createIfAbsent) throws Exception {
        return send(tenantId, "getTenantPartitionRoutingGroup", httpClient -> {
            HttpResponse httpResponse = httpClient.get(
                "/miru/wal/amza/routing/lazyTenantPartition" +
                    "/" + routingGroupType.name() +
                    "/" + tenantId.toString() +
                    "/" + partitionId.getId() +
                    "/" + createIfAbsent,
                null);
            HostPort[] response = responseMapper.extractResultFromResponse(httpResponse, HostPort[].class, null);
            return new ClientResponse<>(response, true);
        });
    }

    @Override
    public List<MiruTenantId> getAllTenantIds() throws Exception { //TODO finish this
        return send(new MiruTenantId(new byte[0]), "getAllTenantIds", client -> {
            HttpResponse response = client.get("/miru/wal/amza/tenants/all", null);
            MiruTenantId[] result = responseMapper.extractResultFromResponse(response, MiruTenantId[].class, null);
            return new ClientResponse<>(Arrays.asList(result), true);
        });
    }

    @Override
    public void writeActivity(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        try {
            final String jsonActivities = requestMapper.writeValueAsString(partitionedActivities);
            while (true) {
                try {
                    String result = sendWithTenantPartition(RoutingGroupType.activity, tenantId, partitionId, true, "writeActivity",
                        client -> extract(
                            client.postJson("/miru/wal/amza/write/activities/" + tenantId.toString() + "/" + partitionId.getId(), jsonActivities, null),
                            String.class,
                            null));
                    if (result != null) {
                        return;
                    }
                    sickThreads.sick(new Throwable("Empty response"));
                    LOG.warn("Empty response during write activities for {} {}, retrying in 1 sec", tenantId, partitionId);
                } catch (Exception x) {
                    sickThreads.sick(x);
                    LOG.warn("Failed to write activities for {} {}, retrying in 1 sec", new Object[] { tenantId, partitionId }, x);
                }
                Thread.sleep(1_000);
            }
        } finally {
            sickThreads.recovered();
        }
    }

    @Override
    public void writeReadTracking(MiruTenantId tenantId,
        List<MiruReadEvent> readEvents,
        Function<MiruReadEvent, MiruPartitionedActivity> transformer) throws Exception {

        ListMultimap<MiruStreamId, MiruPartitionedActivity> partitionedActivities = ArrayListMultimap.create();
        for (MiruReadEvent readEvent : readEvents) {
            partitionedActivities.put(readEvent.streamId, transformer.apply(readEvent));
        }

        List<StreamIdPartitionedActivities> streamActivities = Lists.newArrayList();
        for (MiruStreamId streamId : partitionedActivities.keySet()) {
            streamActivities.add(new StreamIdPartitionedActivities(streamId.getBytes(), partitionedActivities.get(streamId)));
        }

        try {
            final String jsonActivities = requestMapper.writeValueAsString(streamActivities);
            while (true) {
                try {
                    String result = sendWithTenant(RoutingGroupType.readTracking, tenantId, true, "writeReadTracking",
                        client -> extract(client.postJson("/miru/wal/amza/write/reads/" + tenantId.toString(), jsonActivities, null),
                            String.class,
                            null));
                    if (result != null) {
                        return;
                    }
                    sickThreads.sick(new Throwable("Empty response"));
                    LOG.warn("Empty response during write read tracking for {}, retrying in 1 sec", tenantId);
                } catch (Exception x) {
                    sickThreads.sick(x);
                    LOG.warn("Failed to write read tracking for {} {}, retrying in 1 sec", new Object[] { tenantId }, x);
                }
                Thread.sleep(1_000);
            }
        } finally {
            sickThreads.recovered();
        }
    }

    @Override
    public MiruPartitionId getLargestPartitionId(final MiruTenantId tenantId) {
        return send(tenantId, "getLargestPartitionId", client -> {
            HttpResponse response = client.get("/miru/wal/amza/largestPartitionId/" + tenantId.toString(), null);
            return new ClientResponse<>(responseMapper.extractResultFromResponse(response, MiruPartitionId.class, null), true);
        });
    }

    @Override
    public WriterCursor getCursorForWriterId(MiruTenantId tenantId, MiruPartitionId partitionId, int writerId) throws Exception {
        return sendWithTenantPartition(RoutingGroupType.activity, tenantId, partitionId, true, "getCursorForWriterId",
            client -> extract(client.get("/miru/wal/amza/cursor/writer/" + tenantId.toString() + "/" + partitionId.getId() + "/" + writerId, null),
                WriterCursor.class,
                null));
    }

    @Override
    public MiruActivityWALStatus getActivityWALStatusForTenant(final MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return sendWithTenantPartition(RoutingGroupType.activity, tenantId, partitionId, false, "getActivityWALStatusForTenant",
            client -> extract(client.get("/miru/wal/amza/activity/wal/status/" + tenantId.toString() + "/" + partitionId.getId(), null),
                MiruActivityWALStatus.class,
                null));
    }

    @Override
    public long oldestActivityClockTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        Long timestamp = sendWithTenantPartition(RoutingGroupType.activity, tenantId, partitionId, false, "oldestActivityClockTimestamp",
            client -> extract(client.get("/miru/wal/amza/oldest/activity/" + tenantId.toString() + "/" + partitionId.getId(), null),
                Long.class,
                null));
        return timestamp != null ? timestamp : -1;
    }

    @Override
    public List<MiruVersionedActivityLookupEntry> getVersionedEntries(MiruTenantId tenantId, MiruPartitionId partitionId, Long[] timestamps) throws Exception {
        final String jsonTimestamps = requestMapper.writeValueAsString(timestamps);
        return sendWithTenantPartition(RoutingGroupType.activity, tenantId, partitionId, false, "getVersionedEntries",
            client -> extractToList(
                client.postJson("/miru/wal/amza/versioned/entries/" + tenantId.toString() + "/" + partitionId.getId(), jsonTimestamps, null),
                MiruVersionedActivityLookupEntry[].class));
    }

    @Override
    public StreamBatch<MiruWALEntry, AmzaSipCursor> sipActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        AmzaSipCursor cursor,
        Set<TimeAndVersion> lastSeen,
        int batchSize) throws Exception {
        final String jsonCursor = requestMapper.writeValueAsString(new SipAndLastSeen<>(cursor, lastSeen));
        try {
            while (true) {
                try {
                    @SuppressWarnings("unchecked")
                    StreamBatch<MiruWALEntry, AmzaSipCursor> response = sendWithTenantPartition(RoutingGroupType.activity, tenantId, partitionId, false,
                        "sipActivity",
                        client -> extract(
                            client.postJson("/miru/wal/amza/sip/activity/" + tenantId.toString() + "/" + partitionId.getId() + "/" + batchSize, jsonCursor,
                                null),
                            StreamBatch.class,
                            new Class[] { MiruWALEntry.class, AmzaSipCursor.class },
                            null));
                    if (response != null) {
                        return response;
                    }
                    sickThreads.sick(new Throwable("Empty response"));
                    LOG.warn("Empty response while streaming, will retry in {} ms", sleepOnFailureMillis);
                } catch (Exception e) {
                    sickThreads.sick(e);
                    LOG.warn("Failure while streaming, will retry in {} ms", new Object[] { sleepOnFailureMillis }, e);
                }
                Thread.sleep(sleepOnFailureMillis);
            }
        } finally {
            sickThreads.recovered();
        }
    }

    @Override
    public long getActivityCount(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        try {
            String endpoint = "/miru/wal/amza/activityCount/" + tenantId.toString() + "/" + partitionId.getId();
            while (true) {
                try {
                    @SuppressWarnings("unchecked")
                    Long response = sendWithTenantPartition(RoutingGroupType.activity, tenantId, partitionId, false,
                        "getActivityCount",
                        client -> {
                            HttpResponse httpResponse = client.postJson(endpoint, "{}", null);
                            return extract(httpResponse, Long.class, null);
                        });
                    if (response != null) {
                        return response;
                    }
                    sickThreads.sick(new Throwable("Empty response"));
                    LOG.warn("Empty response while counting, will retry in {} ms", sleepOnFailureMillis);
                } catch (Exception e) {
                    sickThreads.sick(e);
                    LOG.warn("Failure while counting, will retry in {} ms", new Object[] { sleepOnFailureMillis }, e);
                }
                Thread.sleep(sleepOnFailureMillis);
            }
        } finally {
            sickThreads.recovered();
        }
    }

    @Override
    public StreamBatch<MiruWALEntry, AmzaCursor> getActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        AmzaCursor cursor,
        int batchSize,
        long stopAtTimestamp,
        MutableLong bytesCount) throws Exception {
        try {
            String endpoint = "/miru/wal/amza/activity/" + tenantId.toString() + "/" + partitionId.getId() + "/" + batchSize + "/" + stopAtTimestamp;
            String jsonCursor = requestMapper.writeValueAsString(cursor);
            while (true) {
                try {
                    @SuppressWarnings("unchecked")
                    StreamBatch<MiruWALEntry, AmzaCursor> response = sendWithTenantPartition(RoutingGroupType.activity, tenantId, partitionId, false,
                        "getActivity",
                        client -> {
                            HttpResponse httpResponse = client.postJson(endpoint, jsonCursor, null);
                            if (bytesCount != null && httpResponse.getResponseBody() != null) {
                                bytesCount.add(httpResponse.getResponseBody().length);
                            }
                            return extract(
                                httpResponse,
                                StreamBatch.class,
                                new Class[] { MiruWALEntry.class, AmzaCursor.class },
                                null);
                        });
                    if (response != null) {
                        return response;
                    }
                    sickThreads.sick(new Throwable("Empty response"));
                    LOG.warn("Empty response while streaming, will retry in {} ms", sleepOnFailureMillis);
                } catch (Exception e) {
                    sickThreads.sick(e);
                    LOG.warn("Failure while streaming, will retry in {} ms", new Object[] { sleepOnFailureMillis }, e);
                }
                Thread.sleep(sleepOnFailureMillis);
            }
        } finally {
            sickThreads.recovered();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public OldestReadResult<AmzaSipCursor> oldestReadEventId(MiruTenantId tenantId,
        MiruStreamId streamId,
        AmzaSipCursor cursor,
        boolean createIfAbsent) throws Exception {
        final String jsonCursor = requestMapper.writeValueAsString(cursor);
        return sendWithTenant(RoutingGroupType.readTracking, tenantId, createIfAbsent, "oldestReadEventId",
            client -> extract(
                client.postJson("/miru/wal/amza/oldestReadEventId/" + tenantId.toString() + "/" + streamId.toString(),
                    jsonCursor,
                    null),
                OldestReadResult.class,
                new Class[] { AmzaSipCursor.class },
                null));
    }

    @Override
    @SuppressWarnings("unchecked")
    public StreamBatch<MiruWALEntry, Long> scanRead(MiruTenantId tenantId,
        MiruStreamId streamId,
        long fromTimestamp,
        int batchSize,
        boolean createIfAbsent) throws Exception {
        return sendWithTenant(RoutingGroupType.readTracking, tenantId, createIfAbsent, "scanRead",
            client -> extract(
                client.postJson("/miru/wal/amza/scanRead/" + tenantId.toString() + "/" + streamId.toString() + "/" + fromTimestamp + "/" + batchSize,
                    "{}",
                    null),
                StreamBatch.class,
                new Class[] { MiruWALEntry.class, Long.class },
                null));
    }

    private <R> R send(MiruTenantId miruTenantId, String family, ClientCall<HttpClient, R, HttpClientException> call) {
        try {

            NextClientStrategy nextClientStrategy = tenantNextClientStrategy.computeIfAbsent(miruTenantId,
                (t) -> new TailAtScaleStrategy(tasExecutors, tasWindowSize, tasPercentile, tasInitialSLAMillis));

            return walClient.call(routingTenantId, nextClientStrategy, family, call);
        } catch (Exception x) {
            throw new RuntimeException("Failed to send.", x);
        }
    }

    private <R> R sendWithTenantPartition(RoutingGroupType routingGroupType,
        MiruTenantId tenantId,
        MiruPartitionId partitionId,
        boolean createIfAbsent,
        String family,
        ClientCall<HttpClient, SendResult<R>, HttpClientException> call) throws Exception {
        TenantRoutingGroup<MiruPartitionId> routingGroup = new TenantRoutingGroup<>(routingGroupType, tenantId, partitionId);
        try {
            while (true) {
                NextClientStrategy strategy = tenantRoutingCache.get(routingGroup,
                    () -> {
                        HostPort[] hostPorts = getTenantPartitionRoutingGroup(routingGroupType, tenantId, partitionId, createIfAbsent);
                        if (hostPorts == null || hostPorts.length == 0) {
                            throw new MiruRouteUnavailableException("No route available for tenant " + tenantId + " partition " + partitionId);
                        }
                        return new ConnectionDescriptorSelectiveStrategy(hostPorts);
                    });
                SendResult<R> sendResult = walClient.call(routingTenantId, strategy, family, call);
                if (sendResult.validRoute) {
                    return sendResult.result;
                }

                tenantRoutingCache.invalidate(routingGroup);
                if (sendResult.errorRoute) {
                    return null;
                }

                sickThreads.sick(new Throwable("Tenant partition route is invalid"));
            }
        } catch (Exception x) {
            tenantRoutingCache.invalidate(routingGroup);
            throw x;
        } finally {
            sickThreads.recovered();
        }
    }

    private <R> R sendWithTenant(RoutingGroupType routingGroupType,
        MiruTenantId tenantId,
        boolean createIfAbsent,
        String family,
        ClientCall<HttpClient, SendResult<R>, HttpClientException> call) throws Exception {
        TenantRoutingGroup<Void> routingGroup = new TenantRoutingGroup<>(routingGroupType, tenantId, null);
        try {
            while (true) {
                NextClientStrategy strategy = tenantRoutingCache.get(routingGroup,
                    () -> {
                        HostPort[] hostPorts = getTenantRoutingGroup(routingGroupType, tenantId, createIfAbsent);
                        if (hostPorts == null || hostPorts.length == 0) {
                            throw new MiruRouteUnavailableException("No route available for tenant " + tenantId);
                        }
                        return new ConnectionDescriptorSelectiveStrategy(hostPorts);
                    });
                SendResult<R> sendResult = walClient.call(routingTenantId, strategy, family, call);
                if (sendResult.validRoute) {
                    return sendResult.result;
                }

                tenantRoutingCache.invalidate(routingGroup);
                if (sendResult.errorRoute) {
                    return null;
                }
                sickThreads.sick(new Throwable("Tenant stream route is invalid"));
            }
        } catch (Exception x) {
            tenantRoutingCache.invalidate(routingGroup);
            throw x;
        } finally {
            sickThreads.recovered();
        }
    }

    private static class SendResult<R> {

        public final R result;
        public final boolean validRoute;
        public final boolean errorRoute;

        public SendResult(R result, boolean validRoute, boolean errorRoute) {
            this.result = result;
            this.validRoute = validRoute;
            this.errorRoute = errorRoute;
        }
    }

    private <R> ClientResponse<SendResult<R>> extract(HttpResponse response, Class<R> resultClass, R emptyResult) {
        if (response.getStatusCode() == HttpStatus.SC_NOT_FOUND || response.getStatusCode() == HttpStatus.SC_CONFLICT) {
            return new ClientResponse<>(new SendResult<>(emptyResult, false, false), true);
        } else if (!responseMapper.isSuccessStatusCode(response.getStatusCode())) {
            return new ClientResponse<>(new SendResult<>(emptyResult, false, true), true);
        }
        R result = responseMapper.extractResultFromResponse(response, resultClass, emptyResult);
        return new ClientResponse<>(new SendResult<>(result, true, false), true);
    }

    private <R> ClientResponse<SendResult<List<R>>> extractToList(HttpResponse response, Class<R[]> resultClass) {
        if (response.getStatusCode() == HttpStatus.SC_NOT_FOUND || response.getStatusCode() == HttpStatus.SC_CONFLICT) {
            return new ClientResponse<>(new SendResult<>(null, false, false), true);
        } else if (!responseMapper.isSuccessStatusCode(response.getStatusCode())) {
            return new ClientResponse<>(new SendResult<>(null, false, true), true);
        }
        R[] result = responseMapper.extractResultFromResponse(response, resultClass, null);
        return new ClientResponse<>(new SendResult<>(result != null ? Arrays.asList(result) : null, true, false), true);
    }

    private <R> ClientResponse<SendResult<R>> extract(HttpResponse response, Class<R> resultClass, Class<?>[] typeClasses, R emptyResult) {
        if (response.getStatusCode() == HttpStatus.SC_NOT_FOUND || response.getStatusCode() == HttpStatus.SC_CONFLICT) {
            return new ClientResponse<>(new SendResult<>(null, false, false), true);
        } else if (!responseMapper.isSuccessStatusCode(response.getStatusCode())) {
            return new ClientResponse<>(new SendResult<>(null, false, true), true);
        }
        R result = responseMapper.extractResultFromResponse(response, resultClass, typeClasses, emptyResult);
        return new ClientResponse<>(new SendResult<>(result, true, false), true);
    }

    private static class TenantRoutingGroup<P> {

        private final RoutingGroupType type;
        private final MiruTenantId tenantId;
        private final P payload;

        public TenantRoutingGroup(RoutingGroupType type, MiruTenantId tenantId, P payload) {
            this.type = type;
            this.tenantId = tenantId;
            this.payload = payload;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TenantRoutingGroup<?> that = (TenantRoutingGroup<?>) o;

            if (type != that.type) {
                return false;
            }
            if (tenantId != null ? !tenantId.equals(that.tenantId) : that.tenantId != null) {
                return false;
            }
            return !(payload != null ? !payload.equals(that.payload) : that.payload != null);

        }

        @Override
        public int hashCode() {
            int result = type != null ? type.hashCode() : 0;
            result = 31 * result + (tenantId != null ? tenantId.hashCode() : 0);
            result = 31 * result + (payload != null ? payload.hashCode() : 0);
            return result;
        }
    }

}
