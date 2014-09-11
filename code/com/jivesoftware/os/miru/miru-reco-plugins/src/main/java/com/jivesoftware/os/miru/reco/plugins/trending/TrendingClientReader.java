package com.jivesoftware.os.miru.reco.plugins.trending;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.reader.cluster.MiruClusterReader;

/**
 *
 */
public class TrendingClientReader {

    private final MiruClusterReader clusterReader;

    public TrendingClientReader(MiruClusterReader clusterReader) {
        this.clusterReader = clusterReader;
    }

    public TrendingAnswer scoreTrending(MiruTenantId tenantId,
            Optional<MiruActorId> actorId,
            TrendingQuery query)
            throws MiruQueryServiceException {

        try {
            return clusterReader.read(tenantId, actorId, query,
                    TrendingConstants.TRENDING_PREFIX + TrendingConstants.CUSTOM_QUERY_ENDPOINT,
                    TrendingAnswer.class, TrendingAnswer.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed score trending stream", e);
        }
    }

}
