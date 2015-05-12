package com.jivesoftware.os.miru.reco.plugins.reco;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestAndReport;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionMarshaller;
import com.jivesoftware.os.miru.plugin.solution.MiruSolvableFactory;
import com.jivesoftware.os.miru.reco.plugins.distincts.Distincts;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsAnswer;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsAnswerEvaluator;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsAnswerMerger;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuestion;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsReport;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class RecoInjectable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruProvider provider;
    private final CollaborativeFiltering collaborativeFiltering;
    private final Distincts distincts;
    private final MiruSolutionMarshaller<DistinctsQuery, DistinctsAnswer, DistinctsReport> distinctsMarshaller;
    private final MiruSolutionMarshaller<RecoQuery, RecoAnswer, RecoReport> marshaller;

    public RecoInjectable(MiruProvider<? extends Miru> provider,
        CollaborativeFiltering collaborativeFiltering,
        Distincts distincts,
        MiruSolutionMarshaller<DistinctsQuery, DistinctsAnswer, DistinctsReport> distinctsMarshaller,
        MiruSolutionMarshaller<RecoQuery, RecoAnswer, RecoReport> marshaller) {
        this.provider = provider;
        this.collaborativeFiltering = collaborativeFiltering;
        this.distincts = distincts;
        this.distinctsMarshaller = distinctsMarshaller;
        this.marshaller = marshaller;
    }

    public MiruResponse<RecoAnswer> collaborativeFilteringRecommendations(MiruRequest<RecoQuery> request) throws MiruQueryServiceException {
        try {
            LOG.debug("askAndMerge: request={}", request);
            MiruTenantId tenantId = request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            MiruFilter removeDistinctsFilter = MiruFilter.NO_FILTER;
            if (request.query.removeDistinctsQuery != null) {
                MiruResponse<DistinctsAnswer> distinctsResponse = miru.askAndMerge(tenantId,
                    new MiruSolvableFactory<>(provider.getStats(), "recoDistincts", new DistinctsQuestion(distincts, new MiruRequest<>(
                                request.tenantId,
                                request.actorId,
                                request.authzExpression,
                                request.query.removeDistinctsQuery,
                                request.logLevel)), distinctsMarshaller),
                    new DistinctsAnswerEvaluator(),
                    new DistinctsAnswerMerger(),
                    DistinctsAnswer.EMPTY_RESULTS,
                    request.logLevel);

                List<String> distinctTerms = (distinctsResponse.answer != null && distinctsResponse.answer.results != null)
                    ? distinctsResponse.answer.results
                    : Collections.<String>emptyList();
                if (!distinctTerms.isEmpty()) {
                    removeDistinctsFilter = new MiruFilter(MiruFilterOperation.and,
                        false,
                        Collections.singletonList(new MiruFieldFilter(
                                MiruFieldType.primary, request.query.removeDistinctsQuery.gatherDistinctsForField, distinctTerms)),
                        null);
                }
            }

            return miru.askAndMerge(tenantId,
                new MiruSolvableFactory<>(provider.getStats(), "collaborativeFilteringRecommendations", new RecoQuestion(collaborativeFiltering,
                        request,
                        removeDistinctsFilter), marshaller),
                new RecoAnswerEvaluator(request.query),
                new RecoAnswerMerger(request.query.desiredNumberOfDistincts),
                RecoAnswer.EMPTY_RESULTS,
                request.logLevel);
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
            Miru miru = provider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                partitionId,
                new MiruSolvableFactory<>(provider.getStats(), "collaborativeFilteringRecommendations", new RecoQuestion(collaborativeFiltering,
                        requestAndReport.request,
                        requestAndReport.report.removeDistinctsFilter), marshaller),
                Optional.fromNullable(requestAndReport.report),
                RecoAnswer.EMPTY_RESULTS,
                requestAndReport.request.logLevel);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score reco stream for partition: " + partitionId.getId(), e);
        }
    }

}
