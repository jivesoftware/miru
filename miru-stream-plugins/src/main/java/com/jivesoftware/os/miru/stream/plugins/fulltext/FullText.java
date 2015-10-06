package com.jivesoftware.os.miru.stream.plugins.fulltext;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextAnswer.ActivityScore;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.TreeSet;

/**
 *
 */
public class FullText {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruProvider miruProvider;

    public FullText(MiruProvider miruProvider) {
        this.miruProvider = miruProvider;
    }

    public MiruFilter parseQuery(String defaultField, String query) throws Exception {
        return miruProvider.getQueryParser(defaultField).parse(query);
    }

    public <BM> FullTextAnswer getActivityScores(MiruBitmaps<BM> bitmaps,
        MiruRequestContext<BM, ?> requestContext,
        MiruRequest<FullTextQuery> request,
        Optional<FullTextReport> lastReport,
        BM answer) throws Exception {

        log.debug("Get full text for answer={} request={}", answer, request);
        int desiredNumberOfResults = request.query.desiredNumberOfResults;

        ImmutableList.Builder<ActivityScore> activityScores = ImmutableList.builder();
        int alreadyScoredCount = lastReport.isPresent() ? lastReport.get().scoredActivities : 0;
        int collectedResults = alreadyScoredCount;
        MiruActivityInternExtern internExtern = miruProvider.getActivityInternExtern(request.tenantId);
        if (request.query.strategy == FullTextQuery.Strategy.TF_IDF) {
            TreeSet<ActivityScore> scored = new TreeSet<>();
            MiruIntIterator iter = bitmaps.intIterator(answer);
            float minScore = lastReport.isPresent() ? lastReport.get().lowestScore : -Float.MAX_VALUE;
            int acceptableBelowMin = desiredNumberOfResults - alreadyScoredCount;
            while (iter.hasNext()) {
                int lastSetBit = iter.next();
                MiruInternalActivity activity = requestContext.getActivityIndex().get(request.tenantId, lastSetBit);
                float score = score(request, activity);
                if (score > minScore) {
                    ActivityScore activityScore = new ActivityScore(internExtern.extern(activity, requestContext.getSchema()), score);
                    scored.add(activityScore);
                } else if (acceptableBelowMin > 0) {
                    ActivityScore activityScore = new ActivityScore(internExtern.extern(activity, requestContext.getSchema()), score);
                    scored.add(activityScore);
                    acceptableBelowMin--;
                }
                while (scored.size() > desiredNumberOfResults) {
                    scored.pollLast();
                }
            }
            activityScores.addAll(scored);
        } else if (request.query.strategy == FullTextQuery.Strategy.TIME) {
            MiruIntIterator iter = bitmaps.descendingIntIterator(answer);
            while (iter.hasNext()) {
                int lastSetBit = iter.next();
                MiruInternalActivity activity = requestContext.getActivityIndex().get(request.tenantId, lastSetBit);
                float score = score(request, activity);
                ActivityScore activityScore = new ActivityScore(internExtern.extern(activity, requestContext.getSchema()), score);
                activityScores.add(activityScore);
                collectedResults++;
                if (collectedResults >= desiredNumberOfResults) {
                    break;
                }
            }
        }

        ImmutableList<ActivityScore> results = activityScores.build();
        boolean resultsExhausted = request.query.strategy == FullTextQuery.Strategy.TIME &&
            request.query.timeRange.smallestTimestamp > requestContext.getTimeIndex().getLargestTimestamp();

        FullTextAnswer result = new FullTextAnswer(results, resultsExhausted);
        log.debug("result={}", result);
        return result;
    }

    private float score(MiruRequest<FullTextQuery> request, MiruInternalActivity activity) {
        if (request.query.strategy == FullTextQuery.Strategy.TF_IDF) {
            return 0f; //TODO
        } else if (request.query.strategy == FullTextQuery.Strategy.TIME) {
            return (float) activity.time / (float) Long.MAX_VALUE;
        } else {
            throw new IllegalArgumentException("No score for strategy " + request.query.strategy);
        }
    }
}
