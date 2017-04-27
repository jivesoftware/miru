package com.jivesoftware.os.miru.stream.plugins.strut;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkModel;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery.CatwalkFeature;
import com.jivesoftware.os.miru.catwalk.shared.FeatureScore;
import com.jivesoftware.os.miru.catwalk.shared.StrutModel;
import com.jivesoftware.os.miru.catwalk.shared.StrutModelKey;
import com.jivesoftware.os.miru.catwalk.shared.StrutModelScore;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.HttpStreamResponse;
import com.jivesoftware.os.routing.bird.http.client.TailAtScaleStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.routing.bird.shared.ClientCall.ClientResponse;
import com.jivesoftware.os.routing.bird.shared.NextClientStrategy;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import org.xerial.snappy.SnappyInputStream;

/**
 * @author jonathan.colt
 */
public class StrutModelCache {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final TenantAwareHttpClient<String> catwalkClient;
    private final ExecutorService tasExecutors;
    private final int tasWindowSize;
    private final float tasPercentile;
    private final long tasInitialSLAMillis;
    private final ObjectMapper requestMapper;
    private final HttpResponseMapper responseMapper;
    private final Cache<String, byte[]> modelCache;

    private final Map<MiruTenantId, NextClientStrategy> tenantNextClientStrategy = Maps.newConcurrentMap();

    public StrutModelCache(TenantAwareHttpClient<String> catwalkClient,
        ExecutorService tasExecutors,
        int tasWindowSize,
        float tasPercentile,
        long tasInitialSLAMillis,
        ObjectMapper requestMapper,
        HttpResponseMapper responseMapper,
        Cache<String, byte[]> modelCache) {

        this.catwalkClient = catwalkClient;
        this.tasExecutors = tasExecutors;
        this.tasWindowSize = tasWindowSize;
        this.tasPercentile = tasPercentile;
        this.tasInitialSLAMillis = tasInitialSLAMillis;
        this.requestMapper = requestMapper;
        this.responseMapper = responseMapper;
        this.modelCache = modelCache;
    }

    private static class ModelNotAvailable extends RuntimeException {

        public ModelNotAvailable(String message) {
            super(message);
        }
    }

    public StrutModel get(MiruTenantId tenantId,
        String catwalkId,
        String modelId,
        int partitionId,
        CatwalkQuery catwalkQuery) throws Exception {

        NextClientStrategy nextClientStrategy = tenantNextClientStrategy.computeIfAbsent(tenantId,
            (t) -> new TailAtScaleStrategy(tasExecutors, tasWindowSize, tasPercentile, tasInitialSLAMillis));

        String key = tenantId.toString() + "/" + catwalkId + "/" + modelId;
        if (modelCache == null) {
            return convert(catwalkQuery, fetchModel(nextClientStrategy, catwalkQuery, key, partitionId));
        }

        StrutModel model = null;
        byte[] modelBytes = modelCache.getIfPresent(key);
        if (modelBytes != null) {
            SnappyInputStream in = new SnappyInputStream(new BufferedInputStream(new ByteArrayInputStream(modelBytes), 8192));
            CatwalkModel catwalkModel = requestMapper.readValue(in, CatwalkModel.class);
            model = convert(catwalkQuery, catwalkModel);
        } else {
            LOG.inc("strut>model>cache>hit");
        }

        if (model == null) {
            try {
                LOG.inc("strut>model>cache>miss");
                modelBytes = modelCache.get(key, () -> {
                    return fetchModelBytes(nextClientStrategy, catwalkQuery, key, partitionId);
                });

                SnappyInputStream in = new SnappyInputStream(new BufferedInputStream(new ByteArrayInputStream(modelBytes), 8192));
                CatwalkModel catwalkModel = requestMapper.readValue(in, CatwalkModel.class);
                model = convert(catwalkQuery, catwalkModel);
            } catch (ExecutionException ee) {
                if (ee.getCause() instanceof ModelNotAvailable) {
                    LOG.info(ee.getCause().getMessage());
                    return null;
                }
                throw ee;
            }

            if (model.model == null) {
                LOG.info("Discarded null model for tenantId:{} partitionId:{} catwalkId:{} modelId:{}", tenantId, partitionId, catwalkId, modelId);
                modelCache.invalidate(key);
                return null;
            } else {
                boolean empty = true;
                for (Map<StrutModelKey, StrutModelScore> featureModel : model.model) {
                    if (!featureModel.isEmpty()) {
                        empty = false;
                        break;
                    }
                }
                if (empty) {
                    LOG.info("Discarded empty model for tenantId:{} partitionId:{} catwalkId:{} modelId:{}", tenantId, partitionId, catwalkId, modelId);
                    modelCache.invalidate(key);
                    return null;
                }
            }
        } else {

            String json = requestMapper.writeValueAsString(catwalkQuery);
            catwalkClient.call("",
                nextClientStrategy,
                "strutModelCacheUpdate",
                (c) -> new ClientCall.ClientResponse<>(c.postJson("/miru/catwalk/model/update/" + key + "/" + partitionId, json, null), true));
        }
        return model;

    }

    private byte[] fetchModelBytes(NextClientStrategy nextClientStrategy, CatwalkQuery catwalkQuery, String key, int partitionId) throws Exception {

        String json = requestMapper.writeValueAsString(catwalkQuery);
        HttpResponse response = catwalkClient.call("",
            nextClientStrategy,
            "strutModelCacheGet",
            (c) -> new ClientResponse<>(c.postJson("/miru/catwalk/model/get/" + key + "/" + partitionId, json, null), true));
        if (responseMapper.isSuccessStatusCode(response.getStatusCode())) {
            return response.getResponseBody();
        } else {
            throw new ModelNotAvailable("Model not available,"
                + " status code: " + response.getStatusCode()
                + " reason: " + response.getStatusReasonPhrase());
        }
    }

    private CatwalkModel fetchModel(NextClientStrategy nextClientStrategy, CatwalkQuery catwalkQuery, String key, int partitionId) throws Exception {

        String json = requestMapper.writeValueAsString(catwalkQuery);
        HttpStreamResponse response = catwalkClient.call("",
            nextClientStrategy,
            "strutModelCacheGet",
            (c) -> new ClientResponse<>(c.streamingPost("/miru/catwalk/model/get/" + key + "/" + partitionId, json, null), true));

        CatwalkModel catwalkModel = null;
        try {
            if (responseMapper.isSuccessStatusCode(response.getStatusCode())) {
                SnappyInputStream in = new SnappyInputStream(new BufferedInputStream(response.getInputStream(), 8192));
                catwalkModel = requestMapper.readValue(in, CatwalkModel.class);
            }
        } finally {
            response.close();
        }

        if (catwalkModel == null) {
            throw new ModelNotAvailable("Model not available,"
                + " status code: " + response.getStatusCode()
                + " reason: " + response.getStatusReasonPhrase());
        }
        return catwalkModel;
    }

    private StrutModel convert(CatwalkQuery catwalkQuery, CatwalkModel model) {

        CatwalkFeature[] features = catwalkQuery.definition.features;
        @SuppressWarnings("unchecked")
        Map<StrutModelKey, StrutModelScore>[] modelFeatureScore = new Map[features.length];
        for (int i = 0; i < modelFeatureScore.length; i++) {
            modelFeatureScore[i] = new HashMap<>();
        }
        for (int i = 0; i < features.length; i++) {
            if (model != null && model.featureScores != null && model.featureScores[i] != null) {
                List<FeatureScore> featureScores = model.featureScores[i];
                for (FeatureScore featureScore : featureScores) {
                    // magical deflation
                    long denominator = (featureScore.denominator * model.totalNumPartitions[i]) / featureScore.numPartitions;
                    modelFeatureScore[i].put(new StrutModelKey(featureScore.termIds), new StrutModelScore(featureScore.numerators, denominator));
                }
            }
        }
        return new StrutModel(modelFeatureScore,
            model != null ? model.modelCounts : new long[features.length],
            model != null ? model.totalCount : 0,
            model != null ? model.numberOfModels : new int[features.length],
            model != null ? model.totalNumPartitions : new int[features.length]
        );
    }

}
