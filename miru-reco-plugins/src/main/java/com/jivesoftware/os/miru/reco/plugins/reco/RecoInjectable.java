package com.jivesoftware.os.miru.reco.plugins.reco;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestAndReport;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolvableFactory;
import com.jivesoftware.os.miru.reco.plugins.distincts.Distincts;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsAnswer;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsAnswerEvaluator;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsAnswerMerger;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuestion;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsRemotePartition;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class RecoInjectable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruProvider<? extends Miru> provider;
    private final CollaborativeFiltering collaborativeFiltering;
    private final Distincts distincts;

    public RecoInjectable(MiruProvider<? extends Miru> provider,
        CollaborativeFiltering collaborativeFiltering,
        Distincts distincts) {
        this.provider = provider;
        this.collaborativeFiltering = collaborativeFiltering;
        this.distincts = distincts;
    }

    public MiruResponse<RecoAnswer> collaborativeFilteringRecommendations(MiruRequest<RecoQuery> request)
        throws MiruQueryServiceException, InterruptedException {
        try {
            LOG.debug("askAndMerge: request={}", request);
            MiruTenantId tenantId = request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            List<MiruValue> distinctTerms = Collections.emptyList();
            if (request.query.removeDistinctsQuery != null) {
                MiruResponse<DistinctsAnswer> distinctsResponse = miru.askAndMerge(tenantId,
                    new MiruSolvableFactory<>(request.name, provider.getStats(), "collaborativeFilteringDistincts",
                        new DistinctsQuestion(distincts,
                            new MiruRequest<>(
                                request.name,
                                request.tenantId,
                                request.actorId,
                                request.authzExpression,
                                request.query.removeDistinctsQuery,
                                request.logLevel),
                            provider.getRemotePartition(DistinctsRemotePartition.class))),
                    new DistinctsAnswerEvaluator(),
                    new DistinctsAnswerMerger(),
                    DistinctsAnswer.EMPTY_RESULTS,
                    miru.getDefaultExecutor(),
                    request.logLevel);

                if (distinctsResponse.answer != null && distinctsResponse.answer.results != null) {
                    distinctTerms = distinctsResponse.answer.results;
                }
            }

            return miru.askAndMerge(tenantId,
                new MiruSolvableFactory<>(request.name, provider.getStats(), "collaborativeFilteringRecommendations", new RecoQuestion(collaborativeFiltering,
                    request,
                    provider.getRemotePartition(RecoRemotePartition.class),
                    distinctTerms)),
                new RecoAnswerEvaluator(request.query),
                new RecoAnswerMerger(request.query.desiredNumberOfDistincts),
                RecoAnswer.EMPTY_RESULTS,
                miru.getDefaultExecutor(),
                request.logLevel);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score reco stream", e);
        }
    }

    public MiruPartitionResponse<RecoAnswer> collaborativeFilteringRecommendations(MiruPartitionId partitionId,
        MiruRequestAndReport<RecoQuery, RecoReport> requestAndReport)
        throws MiruQueryServiceException, InterruptedException {
        try {
            MiruTenantId tenantId = requestAndReport.request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                partitionId,
                new MiruSolvableFactory<>(requestAndReport.request.name, provider.getStats(), "collaborativeFilteringRecommendations",
                    new RecoQuestion(collaborativeFiltering,
                        requestAndReport.request,
                        provider.getRemotePartition(RecoRemotePartition.class),
                        requestAndReport.report.removeDistincts)),
                Optional.fromNullable(requestAndReport.report),
                RecoAnswer.EMPTY_RESULTS,
                requestAndReport.request.logLevel);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score reco stream for partition: " + partitionId.getId(), e);
        }
    }

}
