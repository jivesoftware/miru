package com.jivesoftware.os.miru.service.partition;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.http.client.HttpClient;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.partition.MiruQueryablePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** @author jonathan */
public class MiruRemoteQueryablePartitionFactory {

    private final HttpClientFactory httpClientFactory;
    private final ObjectMapper objectMapper;
    private final Map<MiruHost, RequestHelper> hostHelpers = new ConcurrentHashMap<>();

    public MiruRemoteQueryablePartitionFactory(HttpClientFactory httpClientFactory,
        ObjectMapper objectMapper) {
        this.httpClientFactory = httpClientFactory;
        this.objectMapper = objectMapper;
    }

    private RequestHelper hostHelper(final MiruPartitionCoord coord) {
        RequestHelper helper = hostHelpers.get(coord.host);
        if (helper == null) {
            HttpClient httpClient = httpClientFactory.createClient(coord.host.getLogicalName(), coord.host.getPort());
            helper = new RequestHelper(httpClient, objectMapper);
            hostHelpers.put(coord.host, helper);
        }
        return helper;
    }

    public <BM, S extends MiruSipCursor<S>> MiruQueryablePartition<BM> create(final MiruPartitionCoord coord, final MiruPartitionCoordInfo info) {

        final RequestHelper requestHelper = hostHelper(coord);

        return new MiruQueryablePartition<BM>() {

            @Override
            public boolean isLocal() {
                return false;
            }

            @Override
            public MiruPartitionCoord getCoord() {
                return coord;
            }

            @Override
            public MiruRequestHandle<BM, S> acquireQueryHandle() throws Exception {
                return new MiruRequestHandle<BM, S>() {

                    @Override
                    public MiruBitmaps<BM> getBitmaps() {
                        return null;
                    }

                    @Override
                    public MiruRequestContext<BM, S> getRequestContext() {
                        return null;
                    }

                    @Override
                    public boolean isLocal() {
                        return false;
                    }

                    @Override
                    public boolean canBackfill() {
                        return false;
                    }

                    @Override
                    public MiruPartitionCoord getCoord() {
                        return coord;
                    }

                    @Override
                    public RequestHelper getRequestHelper() {
                        return requestHelper;
                    }

                    @Override
                    public void close() throws Exception {
                    }
                };
            }
        };
    }
}
