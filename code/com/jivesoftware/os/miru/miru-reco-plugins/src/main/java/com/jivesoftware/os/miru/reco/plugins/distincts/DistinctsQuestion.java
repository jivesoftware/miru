package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruIndexUtil;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.Question;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class DistinctsQuestion implements Question<DistinctsAnswer, DistinctsReport> {

    private final Distincts distincts;
    private final MiruRequest<DistinctsQuery> request;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();
    private final MiruIndexUtil indexUtil = new MiruIndexUtil();

    public DistinctsQuestion(Distincts distincts,
        MiruRequest<DistinctsQuery> request) {
        this.distincts = distincts;
        this.request = request;
    }

    @Override
    public <BM> MiruPartitionResponse<DistinctsAnswer> askLocal(MiruRequestHandle<BM> handle, Optional<DistinctsReport> report) throws Exception {
        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);
        MiruRequestContext<BM> requestContext = handle.getRequestContext();
        MiruBitmaps<BM> bitmaps = handle.getBitmaps();

        int fieldId = requestContext.getSchema().getFieldId(request.query.distinctsAroundField);

        MiruInvertedIndex<BM> invertedIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.latest).get(fieldId,
            indexUtil.makeLatestTerm());
        Optional<BM> optionalIndex = invertedIndex.getIndex();
        BM answer;
        if (optionalIndex.isPresent()) {
            if (MiruFilter.NO_FILTER.equals(request.query.constraintsFilter)) {
                answer = optionalIndex.get();
                solutionLog.log(MiruSolutionLogLevel.INFO, "distincts has no constraintsFilter.");
            } else {
                BM constrained = bitmaps.create();
                aggregateUtil.filter(bitmaps, requestContext.getSchema(), requestContext.getFieldIndexProvider(), request.query.constraintsFilter, solutionLog,
                    constrained, -1);

                BM index = optionalIndex.get();
                List<BM> ands = Arrays.asList(index, constrained);

                // AND it all together to compose an answer
                answer = bitmaps.create();
                bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
                bitmaps.and(answer, ands);
            }
        } else {
            answer = bitmaps.create();
            solutionLog.log(MiruSolutionLogLevel.INFO, "distincts has no index.");
        }

        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "distincts {} items.", bitmaps.cardinality(answer));
            solutionLog.log(MiruSolutionLogLevel.TRACE, "distincts bitmap {}", answer);
        }
        return new MiruPartitionResponse<>(distincts.gather(bitmaps, requestContext, request, answer, report, solutionLog), solutionLog.asList());
    }

    @Override
    public MiruPartitionResponse<DistinctsAnswer> askRemote(RequestHelper requestHelper, MiruPartitionId partitionId, Optional<DistinctsReport> report)
        throws Exception {
        return new DistinctsRemotePartitionReader(requestHelper).gatherDistincts(partitionId, request, report);
    }

    @Override
    public Optional<DistinctsReport> createReport(Optional<DistinctsAnswer> answer) {
        Optional<DistinctsReport> report = Optional.absent();
        if (answer.isPresent()) {
            report = Optional.of(new DistinctsReport(
                answer.get().collectedDistincts));
        }
        return report;
    }
}
