package com.jivesoftware.os.miru.reco.plugins.uniques;

import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolvableFactory;
import com.jivesoftware.os.miru.reco.plugins.distincts.Distincts;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsAnswer;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsAnswerEvaluator;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsAnswerMerger;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuestion;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsRemotePartition;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

/**
 *
 */
public class UniquesInjectable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruProvider<? extends Miru> provider;

    public UniquesInjectable(MiruProvider<? extends Miru> provider) {
        this.provider = provider;
    }

    public MiruResponse<UniquesAnswer> gatherUniques(MiruRequest<UniquesQuery> request) throws MiruQueryServiceException, InterruptedException {
        try {

            UniquesQuery uniquesQuery = request.query;

            DistinctsQuery distinctsQuery = new DistinctsQuery(uniquesQuery.timeRange,
                uniquesQuery.gatherUniquesForField,
                uniquesQuery.gatherDistinctParts,
                uniquesQuery.constraintsFilter,
                uniquesQuery.prefixes);

            MiruRequest<DistinctsQuery> distinctsRequest = new MiruRequest<>(request.name,
                request.tenantId,
                request.actorId,
                request.authzExpression,
                distinctsQuery, request.logLevel);

            MiruResponse<DistinctsAnswer> gatherDistincts = gatherDistincts(distinctsRequest);
            UniquesAnswer uniquesAnswer = new UniquesAnswer(gatherDistincts.answer.collectedDistincts, gatherDistincts.answer.resultsExhausted);

            return new MiruResponse<>(uniquesAnswer, gatherDistincts.solutions,
                gatherDistincts.totalElapsed,
                gatherDistincts.missingSchema,
                gatherDistincts.incompletePartitionIds,
                gatherDistincts.log);

        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to gather uniques", e);
        }
    }

    private MiruResponse<DistinctsAnswer> gatherDistincts(MiruRequest<DistinctsQuery> request) throws MiruQueryServiceException, InterruptedException {
        try {
            Distincts distincts = new Distincts(provider.getTermComposer());

            LOG.debug("askAndMerge: request={}", request);
            MiruTenantId tenantId = request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                new MiruSolvableFactory<>(request.name, provider.getStats(), "gatherDistincts", new DistinctsQuestion(distincts,
                    request,
                    provider.getRemotePartition(DistinctsRemotePartition.class))),
                new DistinctsAnswerEvaluator(),
                new DistinctsAnswerMerger(),
                DistinctsAnswer.EMPTY_RESULTS,
                miru.getDefaultExecutor(),
                request.logLevel);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to gather uniques", e);
        }
    }

}
