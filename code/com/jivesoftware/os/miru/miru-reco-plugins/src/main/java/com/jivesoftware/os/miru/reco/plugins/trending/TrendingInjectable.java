package com.jivesoftware.os.miru.reco.plugins.trending;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.query.Miru;
import com.jivesoftware.os.miru.query.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.query.MiruProvider;
import com.jivesoftware.os.miru.query.MiruSolvableFactory;

/**
 *
 */
public class TrendingInjectable {

    private final MiruProvider<? extends Miru> miruProvider;
    private final Trending trending;

    public TrendingInjectable(MiruProvider<? extends Miru> miruProvider, Trending trending) {
        this.miruProvider = miruProvider;
        this.trending = trending;
    }

    public TrendingResult scoreTrending(TrendingQuery query) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.callAndMerge(tenantId,
                    new MiruSolvableFactory<>(new TrendingExecuteQuery(trending, query)),
                    new TrendingResultEvaluator(query),
                    new MergeTrendingResults(query.desiredNumberOfDistincts),
                    TrendingResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream", e);
        }
    }

    public TrendingResult scoreTrending(MiruPartitionId partitionId,
            TrendingQuery query,
            Optional<TrendingResult> lastResult)
            throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.callAndMerge(tenantId,
                    new MiruSolvableFactory<>(new TrendingExecuteQuery(trending, query)),
                    new TrendingResultEvaluator(query),
                    new MergeTrendingResults(query.desiredNumberOfDistincts),
                    TrendingResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream for partition: " + partitionId.getId(), e);
        }
    }

}
