package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.Question;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class AnalyticsQuestion implements Question<AnalyticsAnswer, AnalyticsReport> {

    private final Analytics analytics;
    private final MiruRequest<AnalyticsQuery> request;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public AnalyticsQuestion(Analytics analytics,
        MiruRequest<AnalyticsQuery> request) {
        this.analytics = analytics;
        this.request = request;
    }

    @Override
    public <BM> MiruPartitionResponse<AnalyticsAnswer> askLocal(MiruRequestHandle<BM> handle, Optional<AnalyticsReport> report) throws Exception {
        MiruSolutionLog solutionLog = new MiruSolutionLog(request.debug);
        MiruRequestContext<BM> stream = handle.getRequestContext();
        MiruBitmaps<BM> bitmaps = handle.getBitmaps();

        // Start building up list of bitmap operations to run
        List<BM> ands = new ArrayList<>();

        MiruTimeRange timeRange = request.query.timeRange;

        // Short-circuit if the time range doesn't live here
        if (!timeIndexIntersectsTimeRange(stream.getTimeIndex(), timeRange)) {
            solutionLog.log("No time index intersection");
            return new MiruPartitionResponse<>(analytics.analyticing(bitmaps, stream, request, report, bitmaps.create(), solutionLog),
                solutionLog.asList());
        }
        ands.add(bitmaps.buildTimeRangeMask(stream.getTimeIndex(), timeRange.smallestTimestamp, timeRange.largestTimestamp));

        // 1) Execute the combined filter above on the given stream, add the bitmap
        BM filtered = bitmaps.create();
        aggregateUtil.filter(bitmaps, stream.getSchema(), stream.getFieldIndex(), request.query.constraintsFilter, filtered, -1);
        ands.add(filtered);

        // 2) Add in the authz check if we have it
        if (!MiruAuthzExpression.NOT_PROVIDED.equals(request.authzExpression)) {
            ands.add(stream.getAuthzIndex().getCompositeAuthz(request.authzExpression));
        }

        // 3) Mask out anything that hasn't made it into the activityIndex yet, or that has been removed from the index
        ands.add(bitmaps.buildIndexMask(stream.getActivityIndex().lastId(), Optional.of(stream.getRemovalIndex().getIndex())));

        // AND it all together and return the results
        BM answer = bitmaps.create();
        bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
        bitmaps.and(answer, ands);

        if (solutionLog.isEnabled()) {
            solutionLog.log("trending {} items.", bitmaps.cardinality(answer));
        }
        return new MiruPartitionResponse<>(analytics.analyticing(bitmaps, stream, request, report, answer, solutionLog), solutionLog.asList());

    }

    @Override
    public MiruPartitionResponse<AnalyticsAnswer> askRemote(RequestHelper requestHelper, MiruPartitionId partitionId, Optional<AnalyticsReport> report)
        throws Exception {
        return new AnalyticsRemotePartitionReader(requestHelper).scoreAnalyticing(partitionId, request, report);
    }

    @Override
    public Optional<AnalyticsReport> createReport(Optional<AnalyticsAnswer> answer) {
        Optional<AnalyticsReport> report = Optional.absent();
        if (answer.isPresent()) {
            report = Optional.of(new AnalyticsReport());
        }
        return report;
    }

    private boolean timeIndexIntersectsTimeRange(MiruTimeIndex timeIndex, MiruTimeRange timeRange) {
        return timeRange.smallestTimestamp <= timeIndex.getLargestTimestamp()
            && timeRange.largestTimestamp >= timeIndex.getSmallestTimestamp();
    }
}
