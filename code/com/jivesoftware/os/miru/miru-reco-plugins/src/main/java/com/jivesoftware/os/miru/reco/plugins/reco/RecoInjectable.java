package com.jivesoftware.os.miru.reco.plugins.reco;

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
public class RecoInjectable {

    private final MiruProvider miruProvider;
    private final CollaborativeFiltering collaborativeFiltering;

    public RecoInjectable(MiruProvider<? extends Miru> miruProvider, CollaborativeFiltering collaborativeFiltering) {
        this.miruProvider = miruProvider;
        this.collaborativeFiltering = collaborativeFiltering;
    }

    public RecoResult collaborativeFilteringRecommendations(RecoQuery query) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.callAndMerge(tenantId,
                    new MiruSolvableFactory<>(new RecoExecuteQuery(collaborativeFiltering, query)),
                    new RecoResultEvaluator(query),
                    new MergeRecoResults(query.resultCount),
                    RecoResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score reco stream", e);
        }
    }

    public RecoResult collaborativeFilteringRecommendations(MiruPartitionId partitionId, RecoQuery query, Optional<RecoResult> lastResult)
            throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.callImmediate(tenantId,
                    partitionId,
                    new MiruSolvableFactory<>(new RecoExecuteQuery(collaborativeFiltering, query)),
                    lastResult,
                    RecoResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score reco stream for partition: " + partitionId.getId(), e);
        }
    }

}
