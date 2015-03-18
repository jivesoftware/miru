package com.jivesoftware.os.miru.stream.plugins.count;

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
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruSolvableFactory;

/**
 *
 */
public class DistinctCountInjectable {

    private final MiruProvider<? extends Miru> miruProvider;
    private final DistinctCount distinctCount;

    public DistinctCountInjectable(MiruProvider<? extends Miru> miruProvider, DistinctCount distinctCount) {
        this.miruProvider = miruProvider;
        this.distinctCount = distinctCount;
    }

    public MiruResponse<DistinctCountAnswer> countCustomStream(MiruRequest<DistinctCountQuery> request) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = request.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                new MiruSolvableFactory<>("countCustomStream", new DistinctCountCustomQuestion(distinctCount, request)),
                new DistinctCountAnswerEvaluator(request.query),
                new DistinctCounterAnswerMerger(),
                DistinctCountAnswer.EMPTY_RESULTS,
                request.logLevel);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count custom stream", e);
        }
    }

    public MiruResponse<DistinctCountAnswer> countInboxStreamAll(MiruRequest<DistinctCountQuery> request) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = request.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                new MiruSolvableFactory<>("countInboxStreamAll", new DistinctCountInboxQuestion(distinctCount,
                    miruProvider.getBackfillerizer(tenantId), request, false)),
                new DistinctCountAnswerEvaluator(request.query),
                new DistinctCounterAnswerMerger(),
                DistinctCountAnswer.EMPTY_RESULTS,
                request.logLevel);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count inbox all stream", e);
        }
    }

    public MiruResponse<DistinctCountAnswer> countInboxStreamUnread(MiruRequest<DistinctCountQuery> request) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = request.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                new MiruSolvableFactory<>("countInboxStreamUnread", new DistinctCountInboxQuestion(distinctCount,
                    miruProvider.getBackfillerizer(tenantId), request, true)),
                new DistinctCountAnswerEvaluator(request.query),
                new DistinctCounterAnswerMerger(),
                DistinctCountAnswer.EMPTY_RESULTS,
                request.logLevel);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count inbox unread stream", e);
        }
    }

    public MiruPartitionResponse<DistinctCountAnswer> countCustomStream(MiruPartitionId partitionId,
        MiruRequestAndReport<DistinctCountQuery, DistinctCountReport> requestAndReport)
        throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = requestAndReport.request.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                partitionId,
                new MiruSolvableFactory<>("countCustomStream", new DistinctCountCustomQuestion(distinctCount, requestAndReport.request)),
                Optional.fromNullable(requestAndReport.report),
                DistinctCountAnswer.EMPTY_RESULTS,
                MiruSolutionLogLevel.NONE);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count custom stream for partition: " + partitionId.getId(), e);
        }
    }

    public MiruPartitionResponse<DistinctCountAnswer> countInboxStreamAll(MiruPartitionId partitionId,
        MiruRequestAndReport<DistinctCountQuery, DistinctCountReport> requestAndReport)
        throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = requestAndReport.request.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                partitionId,
                new MiruSolvableFactory<>("countInboxStreamAll", new DistinctCountInboxQuestion(distinctCount,
                    miruProvider.getBackfillerizer(tenantId), requestAndReport.request, false)),
                Optional.fromNullable(requestAndReport.report),
                DistinctCountAnswer.EMPTY_RESULTS,
                MiruSolutionLogLevel.NONE);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count inbox all stream for partition: " + partitionId.getId(), e);
        }
    }

    public MiruPartitionResponse<DistinctCountAnswer> countInboxStreamUnread(MiruPartitionId partitionId,
        MiruRequestAndReport<DistinctCountQuery, DistinctCountReport> requestAndReport)
        throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = requestAndReport.request.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                partitionId,
                new MiruSolvableFactory<>("countInboxStreamUnread", new DistinctCountInboxQuestion(distinctCount,
                    miruProvider.getBackfillerizer(tenantId), requestAndReport.request, true)),
                Optional.fromNullable(requestAndReport.report),
                DistinctCountAnswer.EMPTY_RESULTS,
                MiruSolutionLogLevel.NONE);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count inbox unread stream for partition: " + partitionId.getId(), e);
        }
    }

}
