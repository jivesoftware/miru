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

    public TrendingResult scoreTrending(MiruTenantId tenantId,
            Optional<MiruActorId> actorId,
            MiruTrendingQueryCriteria queryCriteria)
            throws MiruQueryServiceException {

        MiruTrendingQueryParams params = new MiruTrendingQueryParams(tenantId, queryCriteria);
        try {
            return clusterReader.read(tenantId, actorId, params,
                    TrendingConstants.TRENDING_PREFIX + TrendingConstants.CUSTOM_QUERY_ENDPOINT,
                    TrendingResult.class, TrendingResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed score trending stream", e);
        }
    }

}
