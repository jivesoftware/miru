package com.jivesoftware.os.miru.wal.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.TimeAndVersion;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruVersionedActivityLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.miru.api.wal.SipAndLastSeen;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.health.checkers.SickThreads;
import com.jivesoftware.os.routing.bird.http.client.ConnectionDescriptorSelectiveStrategy;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.shared.HttpClientException;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.routing.bird.shared.ClientCall.ClientResponse;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import com.jivesoftware.os.routing.bird.shared.NextClientStrategy;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.http.HttpStatus;

public class MiruHttpWALClient<C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> implements MiruWALClient<C, S> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String routingTenantId;
    private final TenantAwareHttpClient<String> walClient;
    private final RoundRobinStrategy roundRobinStrategy;
    private final ObjectMapper requestMapper;
    private final HttpResponseMapper responseMapper;
    private final SickThreads sickThreads;
    private final long sleepOnFailureMillis;
    private final String pathPrefix;
    private final Class<C> cursorClass;
    private final Class<S> sipCursorClass;
    private final Cache<TenantRoutingGroup<?>, NextClientStrategy> tenantRoutingCache;

    public MiruHttpWALClient(String routingTenantId,
        TenantAwareHttpClient<String> walClient,
        RoundRobinStrategy roundRobinStrategy,
        ObjectMapper requestMapper,
        HttpResponseMapper responseMapper,
        SickThreads sickThreads, long sleepOnFailureMillis,
        String pathPrefix,
        Class<C> cursorClass,
        Class<S> sipCursorClass) {
        this.routingTenantId = routingTenantId;
        this.walClient = walClient;
        this.roundRobinStrategy = roundRobinStrategy;
        this.requestMapper = requestMapper;
        this.responseMapper = responseMapper;
        this.sickThreads = sickThreads;
        this.sleepOnFailureMillis = sleepOnFailureMillis;
        this.pathPrefix = pathPrefix;
        this.cursorClass = cursorClass;
        this.sipCursorClass = sipCursorClass;
        this.tenantRoutingCache = CacheBuilder.newBuilder()
            .maximumSize(50_000) //TODO config
            .expireAfterWrite(5, TimeUnit.MINUTES) //TODO config
            .build();
    }

    @Override
    public HostPort[] getTenantRoutingGroup(RoutingGroupType routingGroupType, MiruTenantId tenantId) throws Exception {
        return sendRoundRobin("getTenantRoutingGroup", httpClient -> {
            HttpResponse httpResponse = httpClient.get(pathPrefix + "/routing/tenant/" + routingGroupType.name() + "/" + tenantId.toString(), null);
            HostPort[] response = responseMapper.extractResultFromResponse(httpResponse, HostPort[].class, null);
            return new ClientResponse<>(response, true);
        });
    }

    @Override
    public HostPort[] getTenantPartitionRoutingGroup(RoutingGroupType routingGroupType, MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return sendRoundRobin("getTenantPartitionRoutingGroup", httpClient -> {
            HttpResponse httpResponse = httpClient.get(
                pathPrefix + "/routing/tenantPartition/" + routingGroupType.name() + "/" + tenantId.toString() + "/" + partitionId.getId(), null);
            HostPort[] response = responseMapper.extractResultFromResponse(httpResponse, HostPort[].class, null);
            return new ClientResponse<>(response, true);
        });
    }

    @Override
    public HostPort[] getTenantStreamRoutingGroup(RoutingGroupType routingGroupType, MiruTenantId tenantId, MiruStreamId streamId) throws Exception {
        return sendRoundRobin("getTenantStreamRoutingGroup", httpClient -> {
            HttpResponse httpResponse = httpClient.get(
                pathPrefix + "/routing/tenantStream/" + routingGroupType.name() + "/" + tenantId.toString() + "/" + streamId.toString(), null);
            HostPort[] response = responseMapper.extractResultFromResponse(httpResponse, HostPort[].class, null);
            return new ClientResponse<>(response, true);
        });
    }

    @Override
    public List<MiruTenantId> getAllTenantIds() throws Exception { //TODO finish this
        return sendRoundRobin("getAllTenantIds", client -> {
            HttpResponse response = client.get(pathPrefix + "/tenants/all", null);
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
                    String result = sendWithTenantPartition(RoutingGroupType.activity, tenantId, partitionId, "writeActivity",
                        client -> extract(
                            client.postJson(pathPrefix + "/write/activities/" + tenantId.toString() + "/" + partitionId.getId(), jsonActivities, null),
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
    public void writeReadTracking(MiruTenantId tenantId, MiruStreamId streamId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        try {
            final String jsonActivities = requestMapper.writeValueAsString(partitionedActivities);
            while (true) {
                try {
                    String result = sendWithTenantStream(RoutingGroupType.readTracking, tenantId, streamId, "writeReadTracking",
                        client -> extract(client.postJson(pathPrefix + "/write/reads/" + tenantId.toString() + "/" + streamId.toString(), jsonActivities, null),
                            String.class,
                            null));
                    if (result != null) {
                        return;
                    }
                    sickThreads.sick(new Throwable("Empty response"));
                    LOG.warn("Empty response during write read tracking for {} {}, retrying in 1 sec", tenantId, streamId);
                } catch (Exception x) {
                    sickThreads.sick(x);
                    LOG.warn("Failed to write read tracking for {} {}, retrying in 1 sec", new Object[] { tenantId, streamId }, x);
                }
                Thread.sleep(1_000);
            }
        } finally {
            sickThreads.recovered();
        }
    }

    @Override
    public MiruPartitionId getLargestPartitionId(final MiruTenantId tenantId) throws Exception {
        return sendRoundRobin("getLargestPartitionId", client -> {
            HttpResponse response = client.get(pathPrefix + "/largestPartitionId/" + tenantId.toString(), null);
            return new ClientResponse<>(responseMapper.extractResultFromResponse(response, MiruPartitionId.class, null), true);
        });
    }

    @Override
    public WriterCursor getCursorForWriterId(MiruTenantId tenantId, MiruPartitionId partitionId, int writerId) throws Exception {
        return sendWithTenantPartition(RoutingGroupType.activity, tenantId, partitionId, "getCursorForWriterId",
            client -> extract(client.get(pathPrefix + "/cursor/writer/" + tenantId.toString() + "/" + partitionId.getId() + "/" + writerId, null),
                WriterCursor.class,
                null));
    }

    @Override
    public MiruActivityWALStatus getActivityWALStatusForTenant(final MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return sendWithTenantPartition(RoutingGroupType.activity, tenantId, partitionId, "getActivityWALStatusForTenant",
            client -> extract(client.get(pathPrefix + "/activity/wal/status/" + tenantId.toString() + "/" + partitionId.getId(), null),
                MiruActivityWALStatus.class,
                null));
    }

    @Override
    public long oldestActivityClockTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        Long timestamp = sendWithTenantPartition(RoutingGroupType.activity, tenantId, partitionId, "oldestActivityClockTimestamp",
            client -> extract(client.get(pathPrefix + "/oldest/activity/" + tenantId.toString() + "/" + partitionId.getId(), null),
                Long.class,
                null));
        return timestamp != null ? timestamp : -1;
    }

    @Override
    public List<MiruVersionedActivityLookupEntry> getVersionedEntries(MiruTenantId tenantId, MiruPartitionId partitionId, Long[] timestamps) throws Exception {
        final String jsonTimestamps = requestMapper.writeValueAsString(timestamps);
        return sendWithTenantPartition(RoutingGroupType.activity, tenantId, partitionId, "getVersionedEntries",
            client -> extractToList(
                client.postJson(pathPrefix + "/versioned/entries/" + tenantId.toString() + "/" + partitionId.getId(), jsonTimestamps, null),
                MiruVersionedActivityLookupEntry[].class));
    }

    @Override
    public StreamBatch<MiruWALEntry, S> sipActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        S cursor,
        Set<TimeAndVersion> lastSeen,
        int batchSize) throws Exception {
        final String jsonCursor = requestMapper.writeValueAsString(new SipAndLastSeen<>(cursor, lastSeen));
        try {
            while (true) {
                try {
                    @SuppressWarnings("unchecked")
                    StreamBatch<MiruWALEntry, S> response = sendWithTenantPartition(RoutingGroupType.activity, tenantId, partitionId, "sipActivity",
                        client -> extract(
                            client.postJson(pathPrefix + "/sip/activity/" + tenantId.toString() + "/" + partitionId.getId() + "/" + batchSize, jsonCursor,
                                null),
                            StreamBatch.class,
                            new Class[] { MiruWALEntry.class, sipCursorClass },
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
    public StreamBatch<MiruWALEntry, C> getActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        C cursor,
        int batchSize,
        long stopAtTimestamp,
        MutableLong bytesCount) throws Exception {
        try {
            String endpoint = pathPrefix + "/activity/" + tenantId.toString() + "/" + partitionId.getId() + "/" + batchSize + "/" + stopAtTimestamp;
            String jsonCursor = requestMapper.writeValueAsString(cursor);
            while (true) {
                try {
                    @SuppressWarnings("unchecked")
                    StreamBatch<MiruWALEntry, C> response = sendWithTenantPartition(RoutingGroupType.activity, tenantId, partitionId, "getActivity",
                        client -> {
                            HttpResponse httpResponse = client.postJson(endpoint, jsonCursor, null);
                            if (bytesCount != null && httpResponse.getResponseBody() != null) {
                                bytesCount.add(httpResponse.getResponseBody().length);
                            }
                            return extract(
                                httpResponse,
                                StreamBatch.class,
                                new Class[] { MiruWALEntry.class, cursorClass },
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
    public StreamBatch<MiruWALEntry, S> getRead(final MiruTenantId tenantId,
        final MiruStreamId streamId,
        S cursor,
        long oldestTimestamp,
        int batchSize) throws Exception {
        final String jsonCursor = requestMapper.writeValueAsString(cursor);
        return sendWithTenantStream(RoutingGroupType.readTracking, tenantId, streamId, "getRead",
            client -> extract(
                client.postJson(pathPrefix + "/read/" + tenantId.toString() + "/" + streamId.toString() + "/" + oldestTimestamp + "/" + batchSize,
                    jsonCursor,
                    null),
                StreamBatch.class,
                new Class[] { MiruWALEntry.class, sipCursorClass },
                null));
    }

    private <R> R sendRoundRobin(String family, ClientCall<HttpClient, R, HttpClientException> call) {
        try {
            return walClient.call(routingTenantId, roundRobinStrategy, family, call);
        } catch (Exception x) {
            throw new RuntimeException("Failed to send.", x);
        }
    }

    private <R> R sendWithTenantPartition(RoutingGroupType routingGroupType,
        MiruTenantId tenantId,
        MiruPartitionId partitionId,
        String family,
        ClientCall<HttpClient, SendResult<R>, HttpClientException> call) throws Exception {
        TenantRoutingGroup<MiruPartitionId> routingGroup = new TenantRoutingGroup<>(routingGroupType, tenantId, partitionId);
        try {
            while (true) {
                NextClientStrategy strategy = tenantRoutingCache.get(routingGroup,
                    () -> {
                        HostPort[] hostPorts = getTenantPartitionRoutingGroup(routingGroupType, tenantId, partitionId);
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

    //TODO the cardinality here is TOO HIGH! replace with prefixes against tenant-specific routing
    private <R> R sendWithTenantStream(RoutingGroupType routingGroupType,
        MiruTenantId tenantId,
        MiruStreamId streamId,
        String family,
        ClientCall<HttpClient, SendResult<R>, HttpClientException> call) throws Exception {
        TenantRoutingGroup<MiruStreamId> routingGroup = new TenantRoutingGroup<>(routingGroupType, tenantId, streamId);
        try {
            while (true) {
                NextClientStrategy strategy = tenantRoutingCache.get(routingGroup,
                    () -> {
                        HostPort[] hostPorts = getTenantStreamRoutingGroup(routingGroupType, tenantId, streamId);
                        if (hostPorts == null || hostPorts.length == 0) {
                            throw new MiruRouteUnavailableException("No route available for tenant " + tenantId + " stream " + streamId);
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
