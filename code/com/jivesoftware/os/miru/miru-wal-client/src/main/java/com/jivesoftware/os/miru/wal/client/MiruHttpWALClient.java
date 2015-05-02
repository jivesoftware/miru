package com.jivesoftware.os.miru.wal.client;

import com.fasterxml.jackson.databind.ObjectMapper;
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
        return send(client -> {
            HttpResponse response = client.get(routingTenantId, "/miru/wal/tenants/all");
            return (List<MiruTenantId>) responseMapper.extractResultFromResponse(response, List.class, new Class[] { MiruTenantId.class }, null);
        });
    }

    @Override
    public MiruPartitionId getLargestPartitionIdAcrossAllWriters(final MiruTenantId tenantId) throws Exception {
        return send(client -> {
            HttpResponse response = client.get(routingTenantId, "/miru/wal/largestPartitionId/" + tenantId.toString());
            return responseMapper.extractResultFromResponse(response, MiruPartitionId.class, null);
        });
    }

    @Override
    public List<MiruActivityWALStatus> getPartitionStatus(final MiruTenantId tenantId, List<MiruPartitionId> partitionIds) throws Exception {
        final String jsonPartitionIds = requestMapper.writeValueAsString(partitionIds);
        return send(client -> {
            HttpResponse response = client.postJson(routingTenantId, "/miru/wal/partition/status/" + tenantId.toString(), jsonPartitionIds);
            return (List<MiruActivityWALStatus>) responseMapper.extractResultFromResponse(response, List.class, new Class[] { MiruActivityWALStatus.class },
                null);
        });
    }

    @Override
    public List<MiruLookupEntry> lookupActivity(final MiruTenantId tenantId, final long afterTimestamp, final int batchSize) throws Exception {
        return send(client -> {
            HttpResponse response = client.get(routingTenantId,
                "/miru/wal/lookup/activity/" + tenantId.toString() + "/" + batchSize + "/" + afterTimestamp);
            return (List<MiruLookupEntry>) responseMapper.extractResultFromResponse(response, List.class, new Class[] { MiruLookupEntry.class }, null);
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
                StreamBatch<MiruWALEntry, SipActivityCursor> response = send(client -> {
                    HttpResponse response1 = client.postJson(routingTenantId,
                        "/miru/wal/sip/activity/" + tenantId.toString() + "/" + partitionId.getId() + "/" + batchSize, jsonCursor);
                    return (StreamBatch<MiruWALEntry, SipActivityCursor>) responseMapper.extractResultFromResponse(response1, StreamBatch.class,
                        new Class[] { MiruWALEntry.class, SipActivityCursor.class }, null);
                });
                if (response != null) {
                    return response;
                }
            } catch (Exception e) {
                // non-interrupts are retried
                LOG.warn("Failure while streaming, will retry in {} ms", new Object[] { sleepOnFailureMillis }, e);
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
                StreamBatch<MiruWALEntry, GetActivityCursor> response = send(client -> {
                    HttpResponse response1 = client.postJson(routingTenantId,
                        "/miru/wal/activity/" + tenantId.toString() + "/" + partitionId.getId() + "/" + batchSize, jsonCursor);
                    return (StreamBatch<MiruWALEntry, GetActivityCursor>) responseMapper.extractResultFromResponse(response1, StreamBatch.class,
                        new Class[] { MiruWALEntry.class, GetActivityCursor.class }, null);
                });
                if (response != null) {
                    return response;
                }
            } catch (Exception e) {
                // non-interrupts are retried
                LOG.warn("Failure while streaming, will retry in {} ms", new Object[] { sleepOnFailureMillis }, e);
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
        return send(client -> {
            HttpResponse response = client.postJson(routingTenantId,
                "/miru/wal/sip/read/" + tenantId.toString() + "/" + streamId.toString() + "/" + batchSize, jsonCursor);
            return (StreamBatch<MiruReadSipEntry, SipReadCursor>) responseMapper.extractResultFromResponse(response, StreamBatch.class,
                new Class[] { MiruReadSipEntry.class, SipReadCursor.class }, null);
        });
    }

    @Override
    public StreamBatch<MiruWALEntry, GetReadCursor> getRead(final MiruTenantId tenantId,
        final MiruStreamId streamId,
        GetReadCursor cursor,
        final int batchSize) throws
        Exception {
        final String jsonCursor = requestMapper.writeValueAsString(cursor);
        return send(client -> {
            HttpResponse response = client.postJson(routingTenantId,
                "/miru/wal/read/" + tenantId.toString() + "/" + streamId.toString() + "/" + batchSize, jsonCursor);
            return (StreamBatch<MiruWALEntry, GetReadCursor>) responseMapper.extractResultFromResponse(response, StreamBatch.class,
                new Class[] { MiruWALEntry.class, GetReadCursor.class }, null);
        });
    }

    static interface HttpCallable<R> {

        R call(TenantAwareHttpClient<String> client) throws Exception;
    }

}
