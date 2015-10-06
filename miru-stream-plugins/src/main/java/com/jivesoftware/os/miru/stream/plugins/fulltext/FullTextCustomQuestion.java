package com.jivesoftware.os.miru.stream.plugins.fulltext;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.Question;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author jonathan
 */
public class FullTextCustomQuestion implements Question<FullTextQuery, FullTextAnswer, FullTextReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final FullText fullText;
    private final MiruRequest<FullTextQuery> request;
    private final MiruRemotePartition<FullTextQuery, FullTextAnswer, FullTextReport> remotePartition;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public FullTextCustomQuestion(FullText fullText,
        MiruRequest<FullTextQuery> request,
        MiruRemotePartition<FullTextQuery, FullTextAnswer, FullTextReport> remotePartition) {
        this.fullText = fullText;
        this.request = request;
        this.remotePartition = remotePartition;
    }

    @Override
    public <BM> MiruPartitionResponse<FullTextAnswer> askLocal(MiruRequestHandle<BM, ?> handle,
        Optional<FullTextReport> report)
        throws Exception {

        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);
        MiruRequestContext<BM, ?> context = handle.getRequestContext();
        MiruBitmaps<BM> bitmaps = handle.getBitmaps();

        MiruTimeRange timeRange = request.query.timeRange;
        if (!context.getTimeIndex().intersects(timeRange)) {
            solutionLog.log(MiruSolutionLogLevel.WARN, "No time index intersection. Partition {}: {} doesn't intersect with {}",
                handle.getCoord().partitionId, context.getTimeIndex(), timeRange);
            return new MiruPartitionResponse<>(fullText.getActivityScores(bitmaps, context, request, report, bitmaps.create()),
                solutionLog.asList());
        }

        MiruFilter filter = fullText.parseQuery(request.query.defaultField, request.query.query);
        if (!MiruFilter.NO_FILTER.equals(request.query.constraintsFilter)) {
            filter = new MiruFilter(MiruFilterOperation.and, false, null,
                Arrays.asList(filter, request.query.constraintsFilter));
        }

        List<BM> ands = new ArrayList<>();

        BM filtered = bitmaps.create();
        aggregateUtil.filter(bitmaps, context.getSchema(), context.getTermComposer(), context.getFieldIndexProvider(), filter, solutionLog, filtered,
            context.getActivityIndex().lastId(), -1);
        ands.add(filtered);

        ands.add(bitmaps.buildIndexMask(context.getActivityIndex().lastId(), context.getRemovalIndex().getIndex()));

        if (!MiruAuthzExpression.NOT_PROVIDED.equals(request.authzExpression)) {
            ands.add(context.getAuthzIndex().getCompositeAuthz(request.authzExpression));
        }

        if (!MiruTimeRange.ALL_TIME.equals(request.query.timeRange)) {
            ands.add(bitmaps.buildTimeRangeMask(context.getTimeIndex(), timeRange.smallestTimestamp, timeRange.largestTimestamp));
        }

        BM answer = bitmaps.create();
        bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
        bitmaps.and(answer, ands);

        return new MiruPartitionResponse<>(fullText.getActivityScores(bitmaps, context, request, report, answer),
            solutionLog.asList());
    }

    @Override
    public MiruPartitionResponse<FullTextAnswer> askRemote(HttpClient httpClient,
        MiruPartitionId partitionId,
        Optional<FullTextReport> report) throws MiruQueryServiceException {
        return remotePartition.askRemote(httpClient, partitionId, request, report);
    }

    @Override
    public Optional<FullTextReport> createReport(Optional<FullTextAnswer> answer) {
        Optional<FullTextReport> report = Optional.absent();
        if (answer.isPresent()) {
            float lowestScore = Float.MAX_VALUE;
            float highestScore = -Float.MAX_VALUE;
            for (FullTextAnswer.ActivityScore result : answer.get().results) {
                lowestScore = Math.min(lowestScore, result.score);
                highestScore = Math.max(highestScore, result.score);
            }

            report = Optional.of(new FullTextReport(
                answer.get().results.size(),
                lowestScore,
                highestScore));
        }
        return report;
    }
}
