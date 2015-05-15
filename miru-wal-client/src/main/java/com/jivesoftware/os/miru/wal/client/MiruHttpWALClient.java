package com.jivesoftware.os.miru.wal.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.http.client.HttpResponse;
import com.jivesoftware.os.jive.utils.http.client.rest.ResponseMapper;
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
import com.jivesoftware.os.upena.tenant.routing.http.client.TenantAwareHttpClient;
import java.util.Collection;
import java.util.List;

public class MiruHttpWALClient<C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> implements MiruWALClient<C, S> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String routingTenantId;
    private final TenantAwareHttpClient<String> client;
    private final ObjectMapper requestMapper;
    private final ResponseMapper responseMapper;
    private final long sleepOnFailureMillis;
    private final String pathPrefix;
    private final Class<C> cursorClass;
    private final Class<S> sipCursorClass;

    public MiruHttpWALClient(String routingTenantId,
        TenantAwareHttpClient<String> client,
        ObjectMapper requestMapper,
        ResponseMapper responseMapper,
        long sleepOnFailureMillis,
        String pathPrefix,
        Class<C> cursorClass,
        Class<S> sipCursorClass) {
        this.routingTenantId = routingTenantId;
        this.client = client;
        this.requestMapper = requestMapper;
        this.responseMapper = responseMapper;
        this.sleepOnFailureMillis = sleepOnFailureMillis;
        this.pathPrefix = pathPrefix;
        this.cursorClass = cursorClass;
        this.sipCursorClass = sipCursorClass;
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
            HttpResponse response = client.get(routingTenantId, pathPrefix + "/tenants/all");
            return (List<MiruTenantId>) responseMapper.extractResultFromResponse(response, List.class, new Class[] { MiruTenantId.class }, null);
        });
    }

    @Override
    public void writeActivityAndLookup(MiruTenantId tenantId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        final String jsonActivities = requestMapper.writeValueAsString(partitionedActivities);
        send(client -> {
            HttpResponse response = client.postJson(routingTenantId, pathPrefix + "/write/activities/" + tenantId.toString(), jsonActivities);
            return responseMapper.extractResultFromResponse(response, String.class, null);
        });
    }

    @Override
    public void writeReadTracking(MiruTenantId tenantId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        final String jsonActivities = requestMapper.writeValueAsString(partitionedActivities);
        send(client -> {
            HttpResponse response = client.postJson(routingTenantId, pathPrefix + "/write/reads/" + tenantId.toString(), jsonActivities);
            return responseMapper.extractResultFromResponse(response, String.class, null);
        });
    }

    @Override
    public MiruPartitionId getLargestPartitionId(final MiruTenantId tenantId) throws Exception {
        return send(client -> {
            HttpResponse response = client.get(routingTenantId, pathPrefix + "/largestPartitionId/" + tenantId.toString());
            return responseMapper.extractResultFromResponse(response, MiruPartitionId.class, null);
        });
    }

    @Override
    public WriterCursor getCursorForWriterId(MiruTenantId tenantId, int writerId) {
        return send(client -> {
            HttpResponse response = client.get(routingTenantId, pathPrefix + "/cursor/writer/" + tenantId.toString() + "/" + writerId);
            return responseMapper.extractResultFromResponse(response, WriterCursor.class, null);
        });
    }

    @Override
    public List<MiruActivityWALStatus> getPartitionStatus(final MiruTenantId tenantId, List<MiruPartitionId> partitionIds) throws Exception {
        final String jsonPartitionIds = requestMapper.writeValueAsString(partitionIds);
        return send(client -> {
            HttpResponse response = client.postJson(routingTenantId, pathPrefix + "/partition/status/" + tenantId.toString(), jsonPartitionIds);
            return (List<MiruActivityWALStatus>) responseMapper.extractResultFromResponse(response, List.class, new Class[] { MiruActivityWALStatus.class },
                null);
        });
    }

    @Override
    public long oldestActivityClockTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return send(client -> {
            HttpResponse response = client.get(routingTenantId, pathPrefix + "/oldest/activity/" + tenantId.toString() + "/" + partitionId.getId());
            return responseMapper.extractResultFromResponse(response, Long.class, null);
        });
    }

    @Override
    public MiruVersionedActivityLookupEntry[] getVersionedEntries(MiruTenantId tenantId, Long[] timestamps) throws Exception {
        final String jsonTimestamps = requestMapper.writeValueAsString(timestamps);
        return send(client -> {
            HttpResponse response = client.postJson(routingTenantId, pathPrefix + "/versioned/entries/" + tenantId.toString(), jsonTimestamps);
            return responseMapper.extractResultFromResponse(response, MiruVersionedActivityLookupEntry[].class, null);
        });
    }

    @Override
    public List<MiruLookupEntry> lookupActivity(final MiruTenantId tenantId, final long afterTimestamp, final int batchSize) throws Exception {
        return send(client -> {
            HttpResponse response = client.get(routingTenantId,
                pathPrefix + "/lookup/activity/" + tenantId.toString() + "/" + batchSize + "/" + afterTimestamp);
            return (List<MiruLookupEntry>) responseMapper.extractResultFromResponse(response, List.class, new Class[] { MiruLookupEntry.class }, null);
        });
    }

    @Override
    public MiruLookupRange lookupRange(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return send(client -> {
            HttpResponse response = client.get(routingTenantId, pathPrefix + "/lookup/range/" + tenantId.toString() + "/" + partitionId.getId());
            return responseMapper.extractResultFromResponse(response, MiruLookupRange.class, null);
        });
    }

    @Override
    public Collection<MiruLookupRange> lookupRanges(MiruTenantId tenantId) throws Exception {
        return send(client -> {
            HttpResponse response = client.get(routingTenantId, pathPrefix + "/lookup/ranges/" + tenantId.toString());
            return (List<MiruLookupRange>) responseMapper.extractResultFromResponse(response, List.class, new Class[] { MiruLookupRange.class }, null);
        });
    }

    @Override
    public StreamBatch<MiruWALEntry, S> sipActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        S cursor,
        int batchSize) throws Exception {
        final String jsonCursor = requestMapper.writeValueAsString(cursor);
        while (true) {
            try {
                StreamBatch<MiruWALEntry, S> response = send(client -> {
                    HttpResponse response1 = client.postJson(routingTenantId,
                        pathPrefix + "/sip/activity/" + tenantId.toString() + "/" + partitionId.getId() + "/" + batchSize, jsonCursor);
                    return (StreamBatch<MiruWALEntry, S>) responseMapper.extractResultFromResponse(response1, StreamBatch.class,
                        new Class[] { MiruWALEntry.class, sipCursorClass }, null);
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
    public StreamBatch<MiruWALEntry, C> getActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        C cursor,
        int batchSize) throws Exception {
        final String jsonCursor = requestMapper.writeValueAsString(cursor);
        while (true) {
            try {
                StreamBatch<MiruWALEntry, C> response = send(client -> {
                    HttpResponse response1 = client.postJson(routingTenantId,
                        pathPrefix + "/activity/" + tenantId.toString() + "/" + partitionId.getId() + "/" + batchSize, jsonCursor);
                    return (StreamBatch<MiruWALEntry, C>) responseMapper.extractResultFromResponse(response1, StreamBatch.class,
                        new Class[] { MiruWALEntry.class, cursorClass }, null);
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
                pathPrefix + "/sip/read/" + tenantId.toString() + "/" + streamId.toString() + "/" + batchSize, jsonCursor);
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
                pathPrefix + "/read/" + tenantId.toString() + "/" + streamId.toString() + "/" + batchSize, jsonCursor);
            return (StreamBatch<MiruWALEntry, GetReadCursor>) responseMapper.extractResultFromResponse(response, StreamBatch.class,
                new Class[] { MiruWALEntry.class, GetReadCursor.class }, null);
        });
    }

    static interface HttpCallable<R> {

        R call(TenantAwareHttpClient<String> client) throws Exception;
    }

}
