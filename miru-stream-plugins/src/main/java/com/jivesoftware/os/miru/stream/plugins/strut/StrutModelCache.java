package com.jivesoftware.os.miru.stream.plugins.strut;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkModel;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkQuery;
import com.jivesoftware.os.miru.stream.plugins.catwalk.FeatureScore;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.routing.bird.shared.ClientCall.ClientResponse;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author jonathan.colt
 */
public class StrutModelCache {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final RoundRobinStrategy robinStrategy = new RoundRobinStrategy();

    private final TenantAwareHttpClient<String> catwalkClient;
    private final ObjectMapper requestMapper;
    private final HttpResponseMapper responseMapper;
    private final Cache<String, StrutModel> modelCache;

    public StrutModelCache(TenantAwareHttpClient<String> catwalkClient,
        ObjectMapper requestMapper,
        HttpResponseMapper responseMapper,
        Cache<String, StrutModel> modelCache) {
        this.catwalkClient = catwalkClient;
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

        String key = tenantId.toString() + "/" + catwalkId + "/" + modelId;

        StrutModel model = modelCache.getIfPresent(key);
        if (model == null) {
            try {
                model = modelCache.get(key, () -> {
                    String json = requestMapper.writeValueAsString(catwalkQuery);
                    HttpResponse response = catwalkClient.call("",
                        robinStrategy,
                        "strutModelCache",
                        (c) -> new ClientResponse<>(c.postJson("/miru/catwalk/model/get/" + key + "/" + partitionId, json, null), true));

                    CatwalkModel catwalkModel = responseMapper.extractResultFromResponse(response, CatwalkModel.class, null);
                    if (catwalkModel == null) {
                        throw new ModelNotAvailable("Model not available,"
                            + " status code: " + response.getStatusCode()
                            + " reason: " + response.getStatusReasonPhrase());
                    }
                    return convert(catwalkQuery, catwalkModel);
                });
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
            } else {
                boolean empty = true;
                for (Map<StrutModelKey, ModelScore> featureModel : model.model) {
                    if (!featureModel.isEmpty()) {
                        empty = false;
                        break;
                    }
                }
                if (empty) {
                    LOG.info("Discarded empty model for tenantId:{} partitionId:{} catwalkId:{} modelId:{}", tenantId, partitionId, catwalkId, modelId);
                    modelCache.invalidate(key);
                }
            }
        } else {
            String json = requestMapper.writeValueAsString(catwalkQuery);
            catwalkClient.call("",
                robinStrategy,
                "strutModelCache",
                (c) -> new ClientCall.ClientResponse<>(c.postJson("/miru/catwalk/model/update/" + key + "/" + partitionId, json, null), true));
        }
        return model;

    }

    private StrutModel convert(CatwalkQuery catwalkQuery, CatwalkModel model) {

        @SuppressWarnings("unchecked")
        Map<StrutModelKey, ModelScore>[] modelFeatureScore = new Map[catwalkQuery.features.length];
        for (int i = 0; i < modelFeatureScore.length; i++) {
            modelFeatureScore[i] = new HashMap<>();
        }
        for (int i = 0; i < catwalkQuery.features.length; i++) {
            if (model != null && model.featureScores != null && model.featureScores[i] != null) {
                List<FeatureScore> featureScores = model.featureScores[i];
                for (FeatureScore featureScore : featureScores) {
                    modelFeatureScore[i].put(new StrutModelKey(featureScore.termIds), new ModelScore(featureScore.numerator, featureScore.denominator));
                }
            }
        }
        return new StrutModel(modelFeatureScore,
            model != null ? model.modelCounts : new long[catwalkQuery.features.length],
            model != null ? model.totalCount : 0,
            model != null ? model.numberOfModels : new int[catwalkQuery.features.length]);
    }

    static class StrutModelKey {

        private final MiruTermId[] termIds;

        StrutModelKey(MiruTermId[] termIds) {
            this.termIds = termIds;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 23 * hash + Arrays.deepHashCode(this.termIds);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final StrutModelKey other = (StrutModelKey) obj;
            if (!Arrays.deepEquals(this.termIds, other.termIds)) {
                return false;
            }
            return true;
        }

    }

    public static class ModelScore {

        public final long numerator;
        public final long denominator;

        public ModelScore(long numerator, long denominator) {
            this.numerator = numerator;
            this.denominator = denominator;
        }
    }

}
