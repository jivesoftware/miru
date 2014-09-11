package com.jivesoftware.os.miru.reco.plugins.trending;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.query.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.query.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.query.context.MiruRequestContext;
import com.jivesoftware.os.miru.query.index.MiruTimeIndex;
import com.jivesoftware.os.miru.query.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.query.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.query.solution.MiruTimeRange;
import com.jivesoftware.os.miru.query.solution.Question;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class TrendingQuestion implements Question<TrendingAnswer, TrendingReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final Trending trending;
    private final TrendingQuery query;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public TrendingQuestion(Trending trending,
            TrendingQuery query) {
        this.trending = trending;
        this.query = query;
    }

    @Override
    public <BM> TrendingAnswer askLocal(MiruRequestHandle<BM> handle, Optional<TrendingReport> report) throws Exception {
        MiruRequestContext<BM> stream = handle.getRequestContext();
        MiruBitmaps<BM> bitmaps = handle.getBitmaps();

        // Start building up list of bitmap operations to run
        List<BM> ands = new ArrayList<>();

        MiruTimeRange timeRange = query.timeRange;

        // Short-circuit if the time range doesn't live here
        if (!timeIndexIntersectsTimeRange(stream.timeIndex, timeRange)) {
            LOG.debug("No time index intersection");
            return trending.trending(bitmaps, stream, query, report, bitmaps.create());
        }
        ands.add(bitmaps.buildTimeRangeMask(stream.timeIndex, timeRange.smallestTimestamp, timeRange.largestTimestamp));

        // 1) Execute the combined filter above on the given stream, add the bitmap
        BM filtered = bitmaps.create();
        aggregateUtil.filter(bitmaps, stream.schema, stream.fieldIndex, query.constraintsFilter, filtered, -1);
        ands.add(filtered);

        // 2) Add in the authz check if we have it
        if (!MiruAuthzExpression.NOT_PROVIDED.equals(query.authzExpression)) {
            ands.add(stream.authzIndex.getCompositeAuthz(query.authzExpression));
        }

        // 3) Mask out anything that hasn't made it into the activityIndex yet, orToSourceSize that has been removed from the index
        ands.add(bitmaps.buildIndexMask(stream.activityIndex.lastId(), Optional.of(stream.removalIndex.getIndex())));

        // AND it all together and return the results
        BM answer = bitmaps.create();
        bitmapsDebug.debug(LOG, bitmaps, "ands", ands);
        bitmaps.and(answer, ands);

        return trending.trending(bitmaps, stream, query, report, answer);
    }

    @Override
    public TrendingAnswer askRemote(RequestHelper requestHelper, MiruPartitionId partitionId, Optional<TrendingReport> report) throws Exception {
        return new TrendingRemotePartitionReader(requestHelper).scoreTrending(partitionId, query, report);
    }

    @Override
    public Optional<TrendingReport> createReport(Optional<TrendingAnswer> answer) {
        Optional<TrendingReport> report = Optional.absent();
        if (answer.isPresent()) {
            report = Optional.of(new TrendingReport(
                    answer.get().aggregateTerms,
                    answer.get().collectedDistincts));
        }
        return report;
    }

    private boolean timeIndexIntersectsTimeRange(MiruTimeIndex timeIndex, MiruTimeRange timeRange) {
        return timeRange.smallestTimestamp <= timeIndex.getLargestTimestamp() &&
                timeRange.largestTimestamp >= timeIndex.getSmallestTimestamp();
    }
}
