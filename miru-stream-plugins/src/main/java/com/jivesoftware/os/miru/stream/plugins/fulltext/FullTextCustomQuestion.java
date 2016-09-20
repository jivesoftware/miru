package com.jivesoftware.os.miru.stream.plugins.fulltext;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.solution.FieldAndTermId;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableInt;

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
    public <BM extends IBM, IBM> MiruPartitionResponse<FullTextAnswer> askLocal(MiruRequestHandle<BM, IBM, ?> handle,
        Optional<FullTextReport> report)
        throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);
        MiruRequestContext<BM, IBM, ?> context = handle.getRequestContext();
        MiruBitmaps<BM, IBM> bitmaps = handle.getBitmaps();

        MiruTimeRange timeRange = request.query.timeRange;
        if (!context.getTimeIndex().intersects(timeRange)) {
            solutionLog.log(MiruSolutionLogLevel.WARN, "No time index intersection. Partition {}: {} doesn't intersect with {}",
                handle.getCoord().partitionId, context.getTimeIndex(), timeRange);
            return new MiruPartitionResponse<>(fullText.getActivityScores("fullText", bitmaps, context, request, report, bitmaps.create(), null),
                solutionLog.asList());
        }

        MiruFilter filter = fullText.parseQuery(request.query.defaultField, request.query.locale, request.query.query);
        Map<FieldAndTermId, MutableInt> termCollector = request.query.strategy == FullTextQuery.Strategy.TF_IDF ? Maps.newHashMap() : null;

        int lastId = context.getActivityIndex().lastId(stackBuffer);
        BM filtered = aggregateUtil.filter("fullTextCustom", bitmaps, context, filter, solutionLog, termCollector, lastId, -1, stackBuffer);

        List<IBM> ands = new ArrayList<>();
        ands.add(filtered);

        ands.add(bitmaps.buildIndexMask(lastId, context.getRemovalIndex(), null, stackBuffer));

        if (!MiruFilter.NO_FILTER.equals(request.query.constraintsFilter)) {
            BM constrained = aggregateUtil.filter("fullTextCustom",
                bitmaps,
                context,
                request.query.constraintsFilter,
                solutionLog,
                null,
                lastId,
                -1,
                stackBuffer);
            ands.add(constrained);
        }

        if (!MiruAuthzExpression.NOT_PROVIDED.equals(request.authzExpression)) {
            ands.add(context.getAuthzIndex().getCompositeAuthz(request.authzExpression, stackBuffer));
        }

        if (!MiruTimeRange.ALL_TIME.equals(request.query.timeRange)) {
            ands.add(bitmaps.buildTimeRangeMask(context.getTimeIndex(), timeRange.smallestTimestamp, timeRange.largestTimestamp, stackBuffer));
        }

        bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
        BM answer = bitmaps.and(ands);

        return new MiruPartitionResponse<>(fullText.getActivityScores("fullText", bitmaps, context, request, report, answer, termCollector),
            solutionLog.asList());
    }

    @Override
    public MiruPartitionResponse<FullTextAnswer> askRemote(MiruHost host,
        MiruPartitionId partitionId,
        Optional<FullTextReport> report) throws MiruQueryServiceException {
        return remotePartition.askRemote(host, partitionId, request, report);
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
