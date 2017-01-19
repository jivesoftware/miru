package com.jivesoftware.os.miru.sync.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.sync.MiruSyncClient;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import java.util.List;
import org.xerial.snappy.Snappy;

/**
 *
 */
public class HttpMiruSyncClient implements MiruSyncClient {

    private final HttpClient httpClient;
    private final ObjectMapper mapper;
    private final String activityPath;
    private final String registerSchemaPath;

    public HttpMiruSyncClient(HttpClient httpClient, ObjectMapper mapper, String activityPath, String registerSchemaPath) {
        this.httpClient = httpClient;
        this.mapper = mapper;
        this.activityPath = activityPath;
        this.registerSchemaPath = registerSchemaPath;
    }

    @Override
    public void writeActivity(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        byte[] bytes = Snappy.compress(mapper.writeValueAsBytes(partitionedActivities));
        String endpoint = activityPath + '/' + tenantId.toString() + '/' + partitionId.getId();
        HttpResponse httpResponse = httpClient.postBytes(endpoint, bytes, null);
        if (!isSuccessStatusCode(httpResponse.getStatusCode())) {
            throw new SyncClientException("Empty response from sync receiver");
        }
    }

    @Override
    public void registerSchema(MiruTenantId tenantId, MiruSchema schema) throws Exception {
        String endpoint = registerSchemaPath + '/' + tenantId.toString();
        String json = mapper.writeValueAsString(schema);
        HttpResponse httpResponse = httpClient.postJson(endpoint, json, null);
        if (!isSuccessStatusCode(httpResponse.getStatusCode())) {
            throw new SyncClientException("Empty response from sync receiver");
        }
    }

    private static boolean isSuccessStatusCode(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }
}
