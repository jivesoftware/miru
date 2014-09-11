package com.jivesoftware.os.miru.stream.plugins.filter;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.query.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.query.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.query.context.MiruRequestContext;
import com.jivesoftware.os.miru.query.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.query.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.query.solution.MiruTimeRange;
import com.jivesoftware.os.miru.query.solution.Question;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author jonathan
 */
public class FilterCustomQuestion implements Question<AggregateCountsAnswer, AggregateCountsReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AggregateCounts aggregateCounts;
    private final AggregateCountsQuery query;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public FilterCustomQuestion(AggregateCounts aggregateCounts, AggregateCountsQuery query) {
        this.aggregateCounts = aggregateCounts;
        this.query = query;
    }

    @Override
    public <BM> AggregateCountsAnswer askLocal(MiruRequestHandle<BM> handle, Optional<AggregateCountsReport> report) throws Exception {
        MiruRequestContext<BM> stream = handle.getRequestContext();
        MiruBitmaps<BM> bitmaps = handle.getBitmaps();

        MiruFilter combinedFilter = query.streamFilter;
        if (!MiruFilter.NO_FILTER.equals(query.constraintsFilter)) {
            combinedFilter = new MiruFilter(MiruFilterOperation.and, Optional.<ImmutableList<MiruFieldFilter>>absent(),
                    Optional.of(ImmutableList.of(query.streamFilter, query.constraintsFilter)));
        }

        List<BM> ands = new ArrayList<>();

        BM filtered = bitmaps.create();
        aggregateUtil.filter(bitmaps, stream.schema, stream.fieldIndex, combinedFilter, filtered, -1);
        ands.add(filtered);

        ands.add(bitmaps.buildIndexMask(stream.activityIndex.lastId(), Optional.of(stream.removalIndex.getIndex())));

        if (!MiruAuthzExpression.NOT_PROVIDED.equals(query.authzExpression)) {
            ands.add(stream.authzIndex.getCompositeAuthz(query.authzExpression));
        }

        if (!MiruTimeRange.ALL_TIME.equals(query.answerTimeRange)) {
            MiruTimeRange timeRange = query.answerTimeRange;
            ands.add(bitmaps.buildTimeRangeMask(stream.timeIndex, timeRange.smallestTimestamp, timeRange.largestTimestamp));
        }

        BM answer = bitmaps.create();
        bitmapsDebug.debug(LOG, bitmaps, "ands", ands);
        bitmaps.and(answer, ands);

        BM counter = null;
        if (!MiruTimeRange.ALL_TIME.equals(query.countTimeRange)) {
            counter = bitmaps.create();
            bitmaps.and(counter, Arrays.asList(answer, bitmaps.buildTimeRangeMask(
                    stream.timeIndex, query.countTimeRange.smallestTimestamp, query.countTimeRange.largestTimestamp)));
        }

        return aggregateCounts.getAggregateCounts(bitmaps, stream, query, report, answer, Optional.fromNullable(counter));
    }

    @Override
    public AggregateCountsAnswer askRemote(RequestHelper requestHelper, MiruPartitionId partitionId, Optional<AggregateCountsReport> report)
            throws Exception {
        return new AggregateCountsRemotePartitionReader(requestHelper).filterCustomStream(partitionId, query, report);
    }

    @Override
    public Optional<AggregateCountsReport> createReport(Optional<AggregateCountsAnswer> answer) {
        Optional<AggregateCountsReport> report = Optional.absent();
        if (answer.isPresent()) {
            report = Optional.of(new AggregateCountsReport(
                    answer.get().aggregateTerms,
                    answer.get().skippedDistincts,
                    answer.get().collectedDistincts));
        }
        return report;
    }
}
