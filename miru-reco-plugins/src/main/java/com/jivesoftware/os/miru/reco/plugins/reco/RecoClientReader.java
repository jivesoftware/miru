package com.jivesoftware.os.miru.reco.plugins.reco;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.reader.cluster.MiruClusterReader;

/**
 *
 */
public class RecoClientReader {

    private final MiruClusterReader clusterReader;

    public RecoClientReader(MiruClusterReader clusterReader) {
        this.clusterReader = clusterReader;
    }

    public RecoAnswer collaborativeFilteringRecommendations(MiruTenantId tenantId,
            Optional<MiruActorId> actorId,
            RecoQuery query)
            throws MiruQueryServiceException {
        try {
            return clusterReader.read(tenantId, actorId, query, RecoConstants.RECO_PREFIX + RecoConstants.CUSTOM_QUERY_ENDPOINT,
                    RecoAnswer.class, RecoAnswer.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed score reco stream", e);
        }
    }

}
