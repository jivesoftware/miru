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

    public RecoResult collaborativeFilteringRecommendations(MiruTenantId tenantId,
            Optional<MiruActorId> actorId,
            MiruRecoQueryCriteria queryCriteria)
            throws MiruQueryServiceException {
        MiruRecoQueryParams params = new MiruRecoQueryParams(tenantId, queryCriteria);
        try {
            return clusterReader.read(tenantId, actorId, params, RecoConstants.RECO_PREFIX + RecoConstants.CUSTOM_QUERY_ENDPOINT,
                    RecoResult.class, RecoResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed score reco stream", e);
        }
    }

}
