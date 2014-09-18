package com.jivesoftware.os.miru.reco.plugins.reco;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestAndReport;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolvableFactory;

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

    public MiruResponse<RecoAnswer> collaborativeFilteringRecommendations(MiruRequest<RecoQuery> request) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = request.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                    new MiruSolvableFactory<>("collaborativeFilteringRecommendations", new RecoQuestion(collaborativeFiltering, request)),
                    new RecoAnswerEvaluator(request.query),
                    new RecoAnswerMerger(request.query.desiredNumberOfDistincts),
                    RecoAnswer.EMPTY_RESULTS, request.debug);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score reco stream", e);
        }
    }

    public MiruPartitionResponse<RecoAnswer> collaborativeFilteringRecommendations(MiruPartitionId partitionId,
            MiruRequestAndReport<RecoQuery, RecoReport> requestAndReport)
            throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = requestAndReport.request.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                    partitionId,
                    new MiruSolvableFactory<>("collaborativeFilteringRecommendations", new RecoQuestion(collaborativeFiltering, requestAndReport.request)),
                    Optional.fromNullable(requestAndReport.report),
                    RecoAnswer.EMPTY_RESULTS, requestAndReport.request.debug);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score reco stream for partition: " + partitionId.getId(), e);
        }
    }

}
