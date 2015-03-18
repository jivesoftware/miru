package com.jivesoftware.os.miru.wal.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.http.client.HttpClientException;
import com.jivesoftware.os.jive.utils.http.client.HttpResponse;
import com.jivesoftware.os.jive.utils.http.client.rest.ResponseMapper;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruReadSipEntry;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.upena.tenant.routing.http.client.TenantAwareHttpClient;
import java.util.List;

public class MiruHttpWALClient implements MiruWALClient {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String routingTenantId;
    private final TenantAwareHttpClient<String> client;
    private final ObjectMapper requestMapper;
    private final ResponseMapper responseMapper;
    private final long sleepOnFailureMillis;

    public MiruHttpWALClient(String routingTenantId,
        TenantAwareHttpClient<String> client,
        ObjectMapper requestMapper,
        ResponseMapper responseMapper,
        long sleepOnFailureMillis) {
        this.routingTenantId = routingTenantId;
        this.client = client;
        this.requestMapper = requestMapper;
        this.responseMapper = responseMapper;
        this.sleepOnFailureMillis = sleepOnFailureMillis;
    }

    private <R> R send(HttpCallable<R> send) {
        try {
            return send.call(client);
        } catch (Exception x) {
            LOG.warn("Failed to send " + send, x);
        }
        throw new RuntimeException("Failed to send.");
    }

    @Override
    public List<MiruTenantId> getAllTenantIds() throws Exception {
        return send(new HttpCallable<List<MiruTenantId>>() {

            @Override
            public List<MiruTenantId> call(TenantAwareHttpClient<String> client) throws HttpClientException {
                HttpResponse response = client.get(routingTenantId, "/miru/wal/tenants/all");
                return responseMapper.extractResultFromResponse(response, List.class, new Class[]{MiruTenantId.class}, null);
            }
        });
    }

    @Override
    public MiruPartitionId getLargestPartitionIdAcrossAllWriters(final MiruTenantId tenantId) throws Exception {
        return send(new HttpCallable<MiruPartitionId>() {

            @Override
            public MiruPartitionId call(TenantAwareHttpClient<String> client) throws HttpClientException {
                HttpResponse response = client.get(routingTenantId, "/miru/wal/largestPartitionId/" + tenantId.toString());
                return responseMapper.extractResultFromResponse(response, MiruPartitionId.class, null);
            }
        });
    }

    @Override
    public List<MiruActivityWALStatus> getPartitionStatus(final MiruTenantId tenantId, List<MiruPartitionId> partitionIds) throws Exception {
        final String jsonPartitionIds = requestMapper.writeValueAsString(partitionIds);
        return send(new HttpCallable<List<MiruActivityWALStatus>>() {

            @Override
            public List<MiruActivityWALStatus> call(TenantAwareHttpClient<String> client) throws HttpClientException {
                HttpResponse response = client.postJson(routingTenantId, "/miru/wal/partition/status/" + tenantId.toString(), jsonPartitionIds);
                return responseMapper.extractResultFromResponse(response, List.class, new Class[]{MiruActivityWALStatus.class}, null);
            }
        });
    }

    @Override
    public List<MiruLookupEntry> lookupActivity(final MiruTenantId tenantId, final long afterTimestamp, final int batchSize) throws Exception {
        return send(new HttpCallable<List<MiruLookupEntry>>() {

            @Override
            public List<MiruLookupEntry> call(TenantAwareHttpClient<String> client) throws HttpClientException {
                HttpResponse response = client.get(routingTenantId,
                    "/miru/wal/lookup/activity/" + tenantId.toString() + "/" + batchSize + "/" + afterTimestamp);
                return responseMapper.extractResultFromResponse(response, List.class, new Class[]{MiruLookupEntry.class}, null);
            }
        });
    }

    @Override
    public StreamBatch<MiruWALEntry, SipActivityCursor> sipActivity(final MiruTenantId tenantId,
        final MiruPartitionId partitionId,
        SipActivityCursor cursor,
        final int batchSize)
        throws Exception {
        final String jsonCursor = requestMapper.writeValueAsString(cursor);
        while (true) {
            try {
                StreamBatch<MiruWALEntry, SipActivityCursor> response = send(new HttpCallable<StreamBatch<MiruWALEntry, SipActivityCursor>>() {

                    @Override
                    public StreamBatch<MiruWALEntry, SipActivityCursor> call(TenantAwareHttpClient<String> client) throws HttpClientException {
                        HttpResponse response = client.postJson(routingTenantId,
                            "/miru/wal/sip/activity/" + tenantId.toString() + "/" + partitionId.getId() + "/" + batchSize, jsonCursor);
                        return responseMapper.extractResultFromResponse(response, StreamBatch.class, new Class[]{MiruWALEntry.class, SipActivityCursor.class},
                            null);
                    }
                });
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
    public StreamBatch<MiruWALEntry, GetActivityCursor> getActivity(final MiruTenantId tenantId,
        final MiruPartitionId partitionId,
        GetActivityCursor cursor,
        final int batchSize)
        throws Exception {
        final String jsonCursor = requestMapper.writeValueAsString(cursor);
        while (true) {
            try {
                StreamBatch<MiruWALEntry, GetActivityCursor> response = send(new HttpCallable<StreamBatch<MiruWALEntry, GetActivityCursor>>() {

                    @Override
                    public StreamBatch<MiruWALEntry, GetActivityCursor> call(TenantAwareHttpClient<String> client) throws HttpClientException {
                        HttpResponse response = client.postJson(routingTenantId,
                            "/miru/wal/activity/" + tenantId.toString() + "/" + partitionId.getId() + "/" + batchSize, jsonCursor);
                        return responseMapper.extractResultFromResponse(response, StreamBatch.class, new Class[]{MiruWALEntry.class, SipActivityCursor.class},
                            null);
                    }
                });
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
    public StreamBatch<MiruReadSipEntry, SipReadCursor> sipRead(final MiruTenantId tenantId,
        final MiruStreamId streamId,
        SipReadCursor cursor,
        final int batchSize) throws
        Exception {
        final String jsonCursor = requestMapper.writeValueAsString(cursor);
        return send(new HttpCallable<StreamBatch<MiruReadSipEntry, SipReadCursor>>() {

            @Override
            public StreamBatch<MiruReadSipEntry, SipReadCursor> call(TenantAwareHttpClient<String> client) throws HttpClientException {
                HttpResponse response = client.postJson(routingTenantId,
                    "/miru/wal/sip/read/" + tenantId.toString() + "/" + streamId.toString() + "/" + batchSize, jsonCursor);
                return responseMapper.extractResultFromResponse(response, StreamBatch.class, new Class[]{MiruReadSipEntry.class, SipReadCursor.class}, null);
            }
        });
    }

    @Override
    public StreamBatch<MiruWALEntry, GetReadCursor> getRead(final MiruTenantId tenantId,
        final MiruStreamId streamId,
        GetReadCursor cursor,
        final int batchSize) throws
        Exception {
        final String jsonCursor = requestMapper.writeValueAsString(cursor);
        return send(new HttpCallable<StreamBatch<MiruWALEntry, GetReadCursor>>() {

            @Override
            public StreamBatch<MiruWALEntry, GetReadCursor> call(TenantAwareHttpClient<String> client) throws HttpClientException {
                HttpResponse response = client.postJson(routingTenantId,
                    "/miru/wal/read/" + tenantId.toString() + "/" + streamId.toString() + "/" + batchSize, jsonCursor);
                return responseMapper.extractResultFromResponse(response, StreamBatch.class, new Class[]{MiruWALEntry.class, GetReadCursor.class}, null);
            }
        });
    }

    static interface HttpCallable<R> {

        R call(TenantAwareHttpClient<String> client) throws Exception;
    }

}
