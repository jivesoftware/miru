package com.jivesoftware.os.miru.stream.plugins.strut;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import java.util.Set;

/**
 *
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

    public StrutModel get(MiruTenantId tenantId,
        String catwalkId,
        String modelId,
        int partitionId,
        CatwalkQuery catwalkQuery) throws Exception {

        String key = tenantId.toString() + "/" + catwalkId + "/" + modelId;

        StrutModel model = modelCache.getIfPresent(key);
        if (model == null) {
            model = modelCache.get(key, () -> {
                String json = requestMapper.writeValueAsString(catwalkQuery);
                HttpResponse response = catwalkClient.call("",
                    robinStrategy,
                    "strutModelCache",
                    (c) -> new ClientResponse<>(c.postJson("/miru/catwalk/model/get/" + key + "/" + partitionId, json, null), true));

                CatwalkModel catwalkModel = responseMapper.extractResultFromResponse(response, CatwalkModel.class, null);
                if (catwalkModel == null) {
                    throw new IllegalStateException("Model not available");
                }
                return convert(catwalkQuery, catwalkModel);
            });

            if (model.model == null) {
                LOG.info("Discarded null model for tenantId:{} partitionId:{} catwalkId:{} modelId:{}", tenantId, partitionId, catwalkId, modelId);
                modelCache.invalidate(key);
            } else {
                boolean empty = true;
                for (Map<StrutModelKey, Float> featureModel : model.model) {
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

        Map<String, Set<MiruTermId>> fieldTerms = Maps.newHashMap();

        @SuppressWarnings("unchecked")
        Map<StrutModelKey, Float>[] modelFeatureScore = new Map[catwalkQuery.featureFields.length];
        for (int i = 0; i < modelFeatureScore.length; i++) {
            modelFeatureScore[i] = new HashMap<>();
        }
        for (int i = 0; i < catwalkQuery.featureFields.length; i++) {
            String[] fields = catwalkQuery.featureFields[i];
            @SuppressWarnings("unchecked")
            Set<MiruTermId>[] terms = new Set[fields.length];
            for (int j = 0; j < fields.length; j++) {
                terms[j] = fieldTerms.computeIfAbsent(fields[j], key -> Sets.newHashSet());
            }
            if (model != null && model.featureScores != null && model.featureScores[i] != null) {
                List<FeatureScore> featureScores = model.featureScores[i];
                for (FeatureScore featureScore : featureScores) {
                    MiruTermId[] termIds = featureScore.termIds;
                    for (int j = 0; j < termIds.length; j++) {
                        terms[j].add(termIds[j]);
                    }
                    modelFeatureScore[i].put(new StrutModelKey(featureScore.termIds), featureScore.numerator / (float) featureScore.denominator);
                }
            }
        }
        return new StrutModel(fieldTerms, modelFeatureScore);
    }

    public static class StrutModelKey {

        private final MiruTermId[] termIds;

        public StrutModelKey(MiruTermId[] termIds) {
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

    public static class StrutModel {

        private final Map<String, Set<MiruTermId>> fieldTerms;
        private final Map<StrutModelKey, Float>[] model;

        public StrutModel(Map<String, Set<MiruTermId>> fieldTerms,
            Map<StrutModelKey, Float>[] model) {
            this.fieldTerms = fieldTerms;
            this.model = model;
        }

        public Map<String, Set<MiruTermId>> getFieldTerms() {
            return fieldTerms;
        }

        public float score(int featureId, MiruTermId[] values, float missing) {
            return model[featureId].getOrDefault(new StrutModelKey(values), missing);
        }
    }
}
