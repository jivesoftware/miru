package com.jivesoftware.os.miru.stream.plugins.fulltext;

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
public class FullTextInjectable {

    private final MiruProvider<? extends Miru> provider;
    private final FullText fullText;

    public FullTextInjectable(MiruProvider<? extends Miru> provider,
        FullText fullText) {
        this.provider = provider;
        this.fullText = fullText;
    }

    public MiruResponse<FullTextAnswer> filterCustomStream(MiruRequest<FullTextQuery> request) throws MiruQueryServiceException, InterruptedException {
        try {
            MiruTenantId tenantId = request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                new MiruSolvableFactory<>(request.name, provider.getStats(),
                    "filterCustomStream",
                    new FullTextCustomQuestion(fullText,
                        request,
                        provider.getRemotePartition(FullTextCustomRemotePartition.class))),
                new FullTextAnswerEvaluator(request.query),
                new FullTextAnswerMerger(request.query.desiredNumberOfResults),
                FullTextAnswer.EMPTY_RESULTS,
                request.logLevel);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter custom stream", e);
        }
    }

    public MiruPartitionResponse<FullTextAnswer> filterCustomStream(MiruPartitionId partitionId,
        MiruRequestAndReport<FullTextQuery, FullTextReport> requestAndReport)
        throws MiruQueryServiceException, InterruptedException {
        try {
            MiruTenantId tenantId = requestAndReport.request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                partitionId,
                new MiruSolvableFactory<>(requestAndReport.request.name, provider.getStats(),
                    "filterCustomStream",
                    new FullTextCustomQuestion(fullText,
                        requestAndReport.request,
                        provider.getRemotePartition(FullTextCustomRemotePartition.class))),
                Optional.fromNullable(requestAndReport.report),
                FullTextAnswer.EMPTY_RESULTS,
                MiruSolutionLogLevel.NONE);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter custom stream for partition: " + partitionId.getId(), e);
        }
    }

}
