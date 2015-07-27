package com.jivesoftware.os.miru.wal.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruReadSipEntry;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruVersionedActivityLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.ConnectionDescriptorSelectiveStrategy;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
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
import java.util.concurrent.TimeUnit;

public class MiruHttpWALClient<C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> implements MiruWALClient<C, S> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String routingTenantId;
    private final TenantAwareHttpClient<String> walClient;
    private final RoundRobinStrategy roundRobinStrategy;
    private final ObjectMapper requestMapper;
    private final HttpResponseMapper responseMapper;
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
        long sleepOnFailureMillis,
        String pathPrefix,
        Class<C> cursorClass,
        Class<S> sipCursorClass) {
        this.routingTenantId = routingTenantId;
        this.walClient = walClient;
        this.roundRobinStrategy = roundRobinStrategy;
        this.requestMapper = requestMapper;
        this.responseMapper = responseMapper;
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
        final String jsonActivities = requestMapper.writeValueAsString(partitionedActivities);
        sendWithTenantPartition(RoutingGroupType.activity, tenantId, partitionId, "writeActivity",
            client -> extract(client.postJson(pathPrefix + "/write/activities/" + tenantId.toString() + "/" + partitionId.getId(), jsonActivities, null),
                String.class));
    }

    @Override
    public void writeLookup(MiruTenantId tenantId, List<MiruVersionedActivityLookupEntry> entries) throws Exception {
        final String jsonEntries = requestMapper.writeValueAsString(entries);
        sendWithTenant(RoutingGroupType.lookup, tenantId, "writeLookup",
            client -> extract(client.postJson(pathPrefix + "/write/lookup/" + tenantId.toString(), jsonEntries, null), String.class));
    }

    @Override
    public void writeReadTracking(MiruTenantId tenantId, MiruStreamId streamId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        final String jsonActivities = requestMapper.writeValueAsString(partitionedActivities);
        sendWithTenant(RoutingGroupType.readTracking, tenantId, "writeReadTracking",
            client -> extract(client.postJson(pathPrefix + "/write/reads/" + tenantId.toString() + "/" + streamId.toString(), jsonActivities, null),
                String.class));
    }

    @Override
    public MiruPartitionId getLargestPartitionId(final MiruTenantId tenantId) throws Exception {
        return sendRoundRobin("getLargestPartitionId", client -> {
            HttpResponse response = client.get(pathPrefix + "/largestPartitionId/" + tenantId.toString(), null);
            return new ClientResponse<>(responseMapper.extractResultFromResponse(response, MiruPartitionId.class, null), true);
        });
    }

    @Override
    public WriterCursor getCursorForWriterId(MiruTenantId tenantId, int writerId) {
        return sendRoundRobin("getCursorForWriterId", client -> {
            HttpResponse response = client.get(pathPrefix + "/cursor/writer/" + tenantId.toString() + "/" + writerId, null);
            return new ClientResponse<>(responseMapper.extractResultFromResponse(response, WriterCursor.class, null), true);
        });
    }

    @Override
    public MiruActivityWALStatus getPartitionStatus(final MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return sendWithTenantPartition(RoutingGroupType.activity, tenantId, partitionId, "getPartitionStatus",
            client -> extract(client.get(pathPrefix + "/partition/status/" + tenantId.toString() + "/" + partitionId.getId(), null),
                MiruActivityWALStatus.class));
    }

    @Override
    public long oldestActivityClockTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return sendRoundRobin("oldestActivityClockTimestamp", client -> {
            HttpResponse response = client.get(pathPrefix + "/oldest/activity/" + tenantId.toString() + "/" + partitionId.getId(), null);
            return new ClientResponse<>(responseMapper.extractResultFromResponse(response, Long.class, null), true);
        });
    }

    @Override
    public List<MiruVersionedActivityLookupEntry> getVersionedEntries(MiruTenantId tenantId, Long[] timestamps) throws Exception {
        final String jsonTimestamps = requestMapper.writeValueAsString(timestamps);
        return sendWithTenant(RoutingGroupType.lookup, tenantId, "getVersionedEntries",
            client -> extractToList(client.postJson(pathPrefix + "/versioned/entries/" + tenantId.toString(), jsonTimestamps, null),
                MiruVersionedActivityLookupEntry[].class));
    }

    @Override
    public List<MiruLookupEntry> lookupActivity(final MiruTenantId tenantId, final long afterTimestamp, final int batchSize) throws Exception {
        return sendWithTenant(RoutingGroupType.lookup, tenantId, "lookupActivity",
            client -> extractToList(client.get(pathPrefix + "/lookup/activity/" + tenantId.toString() + "/" + batchSize + "/" + afterTimestamp, null),
                MiruLookupEntry[].class));
    }

    @Override
    public StreamBatch<MiruWALEntry, S> sipActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        S cursor,
        int batchSize) throws Exception {
        final String jsonCursor = requestMapper.writeValueAsString(cursor);
        while (true) {
            try {
                @SuppressWarnings("unchecked")
                StreamBatch<MiruWALEntry, S> response = sendWithTenantPartition(RoutingGroupType.activity, tenantId, partitionId, "sipActivity",
                    client -> extract(
                        client.postJson(pathPrefix + "/sip/activity/" + tenantId.toString() + "/" + partitionId.getId() + "/" + batchSize, jsonCursor, null),
                        StreamBatch.class,
                        new Class[]{MiruWALEntry.class, sipCursorClass}));
                if (response != null) {
                    return response;
                }
            } catch (Exception e) {
                // non-interrupts are retried
                LOG.warn("Failure while streaming, will retry in {} ms", new Object[]{sleepOnFailureMillis}, e);
                try {
                    Thread.sleep(sleepOnFailureMillis);
                } catch (InterruptedException ie) {
                    Thread.interrupted();
                    throw new RuntimeException("Interrupted during retry after failure, expect partial results");
                }
            }
        }
    }

    @Override
    public StreamBatch<MiruWALEntry, C> getActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        C cursor,
        int batchSize) throws Exception {
        final String jsonCursor = requestMapper.writeValueAsString(cursor);
        while (true) {
            try {
                @SuppressWarnings("unchecked")
                StreamBatch<MiruWALEntry, C> response = sendWithTenantPartition(RoutingGroupType.activity, tenantId, partitionId, "getActivity",
                    client -> extract(
                        client.postJson(pathPrefix + "/activity/" + tenantId.toString() + "/" + partitionId.getId() + "/" + batchSize, jsonCursor, null),
                        StreamBatch.class,
                        new Class[]{MiruWALEntry.class, cursorClass}));
                if (response != null) {
                    return response;
                }
            } catch (Exception e) {
                // non-interrupts are retried
                LOG.warn("Failure while streaming, will retry in {} ms", new Object[]{sleepOnFailureMillis}, e);
                try {
                    Thread.sleep(sleepOnFailureMillis);
                } catch (InterruptedException ie) {
                    Thread.interrupted();
                    throw new RuntimeException("Interrupted during retry after failure, expect partial results");
                }
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public StreamBatch<MiruReadSipEntry, SipReadCursor> sipRead(final MiruTenantId tenantId,
        final MiruStreamId streamId,
        SipReadCursor cursor,
        final int batchSize) throws
        Exception {
        final String jsonCursor = requestMapper.writeValueAsString(cursor);
        return sendWithTenantStream(RoutingGroupType.readTracking, tenantId, streamId, "sipRead",
            client -> extract(client.postJson(pathPrefix + "/sip/read/" + tenantId.toString() + "/" + streamId.toString() + "/" + batchSize, jsonCursor, null),
                StreamBatch.class,
                new Class[]{MiruReadSipEntry.class, SipReadCursor.class}));
    }

    @Override
    @SuppressWarnings("unchecked")
    public StreamBatch<MiruWALEntry, GetReadCursor> getRead(final MiruTenantId tenantId,
        final MiruStreamId streamId,
        GetReadCursor cursor,
        final int batchSize) throws
        Exception {
        final String jsonCursor = requestMapper.writeValueAsString(cursor);
        return sendWithTenant(RoutingGroupType.readTracking, tenantId, "getRead",
            client -> extract(client.postJson(pathPrefix + "/read/" + tenantId.toString() + "/" + streamId.toString() + "/" + batchSize, jsonCursor, null),
                StreamBatch.class,
                new Class[]{MiruWALEntry.class, GetReadCursor.class}));
    }

    private <R> R sendRoundRobin(String family, ClientCall<HttpClient, R, HttpClientException> call) {
        try {
            return walClient.call(routingTenantId, roundRobinStrategy, family, call);
        } catch (Exception x) {
            throw new RuntimeException("Failed to send.", x);
        }
    }

    private <R> R sendWithTenant(RoutingGroupType routingGroupType,
        MiruTenantId tenantId,
        String family,
        ClientCall<HttpClient, SendResult<R>, HttpClientException> call) {
        try {
            TenantRoutingGroup<Void> routingGroup = new TenantRoutingGroup<>(routingGroupType, tenantId, null);
            while (true) {
                NextClientStrategy strategy = tenantRoutingCache.get(routingGroup,
                    () -> new ConnectionDescriptorSelectiveStrategy(getTenantRoutingGroup(routingGroupType, tenantId)));
                SendResult<R> sendResult = walClient.call(routingTenantId, strategy, family, call);
                if (sendResult.resultFound) {
                    return sendResult.result;
                } else {
                    tenantRoutingCache.invalidate(routingGroup);
                }
            }
        } catch (Exception x) {
            throw new RuntimeException("Failed to send.", x);
        }
    }

    private <R> R sendWithTenantPartition(RoutingGroupType routingGroupType,
        MiruTenantId tenantId,
        MiruPartitionId partitionId,
        String family,
        ClientCall<HttpClient, SendResult<R>, HttpClientException> call) {
        try {
            TenantRoutingGroup<MiruPartitionId> routingGroup = new TenantRoutingGroup<>(routingGroupType, tenantId, partitionId);
            while (true) {
                NextClientStrategy strategy = tenantRoutingCache.get(routingGroup,
                    () -> new ConnectionDescriptorSelectiveStrategy(getTenantPartitionRoutingGroup(routingGroupType, tenantId, partitionId)));
                SendResult<R> sendResult = walClient.call(routingTenantId, strategy, family, call);
                if (sendResult.resultFound) {
                    return sendResult.result;
                } else {
                    tenantRoutingCache.invalidate(routingGroup);
                }
            }
        } catch (Exception x) {
            throw new RuntimeException("Failed to send.", x);
        }
    }

    private <R> R sendWithTenantStream(RoutingGroupType routingGroupType,
        MiruTenantId tenantId,
        MiruStreamId streamId,
        String family,
        ClientCall<HttpClient, SendResult<R>, HttpClientException> call) {
        try {
            TenantRoutingGroup<MiruStreamId> routingGroup = new TenantRoutingGroup<>(routingGroupType, tenantId, streamId);
            while (true) {
                NextClientStrategy strategy = tenantRoutingCache.get(routingGroup,
                    () -> new ConnectionDescriptorSelectiveStrategy(getTenantStreamRoutingGroup(routingGroupType, tenantId, streamId)));
                SendResult<R> sendResult = walClient.call(routingTenantId, strategy, family, call);
                if (sendResult.resultFound) {
                    return sendResult.result;
                } else {
                    tenantRoutingCache.invalidate(routingGroup);
                }
            }
        } catch (Exception x) {
            throw new RuntimeException("Failed to send.", x);
        }
    }

    private static class SendResult<R> {

        public final R result;
        public final boolean resultFound;

        public SendResult(R result, boolean resultFound) {
            this.result = result;
            this.resultFound = resultFound;
        }
    }

    private <R> ClientResponse<SendResult<R>> extract(HttpResponse response, Class<R> resultClass) {
        if (response.getStatusCode() == 404) {
            return new ClientResponse<>(new SendResult<>(null, false), true);
        }
        R result = responseMapper.extractResultFromResponse(response, resultClass, null);
        return new ClientResponse<>(new SendResult<>(result, true), true);
    }

    private <R> ClientResponse<SendResult<List<R>>> extractToList(HttpResponse response, Class<R[]> resultClass) {
        if (response.getStatusCode() == 404) {
            return new ClientResponse<>(new SendResult<>(null, false), true);
        }
        R[] result = responseMapper.extractResultFromResponse(response, resultClass, null);
        return new ClientResponse<>(new SendResult<>(Arrays.asList(result), true), true);
    }

    private <R> ClientResponse<SendResult<R>> extract(HttpResponse response, Class<R> resultClass, Class<?>[] typeClasses) {
        if (response.getStatusCode() == 404) {
            return new ClientResponse<>(new SendResult<>(null, false), true);
        }
        R result = responseMapper.extractResultFromResponse(response, resultClass, typeClasses, null);
        return new ClientResponse<>(new SendResult<>(result, true), true);
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
