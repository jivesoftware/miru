package com.jivesoftware.os.miru.reco.plugins.reco;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
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
import com.jivesoftware.os.miru.plugin.solution.Question;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class RecoQuestion implements Question<RecoQuery, RecoAnswer, RecoReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final CollaborativeFiltering collaborativeFiltering;
    private final MiruRequest<RecoQuery> request;
    private final MiruRemotePartition<RecoQuery, RecoAnswer, RecoReport> remotePartition;
    private final MiruFilter removeDistinctsFilter;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public RecoQuestion(CollaborativeFiltering collaborativeFiltering,
        MiruRequest<RecoQuery> query,
        MiruRemotePartition<RecoQuery, RecoAnswer, RecoReport> remotePartition,
        MiruFilter removeDistinctsFilter) {
        this.collaborativeFiltering = Preconditions.checkNotNull(collaborativeFiltering);
        this.request = Preconditions.checkNotNull(query);
        this.remotePartition = remotePartition;
        this.removeDistinctsFilter = Preconditions.checkNotNull(removeDistinctsFilter);
    }

    @Override
    public <BM> MiruPartitionResponse<RecoAnswer> askLocal(MiruRequestHandle<BM, ?> handle, Optional<RecoReport> report) throws Exception {
        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);

        MiruRequestContext<BM, ?> context = handle.getRequestContext();
        MiruBitmaps<BM> bitmaps = handle.getBitmaps();

        if (!context.getTimeIndex().intersects(request.query.timeRange)) {
            solutionLog.log(MiruSolutionLogLevel.WARN,
                "No time index intersection. p=" + handle.getCoord().partitionId + " " + context.getTimeIndex() + " doesn't intersect with " + timeRange);
            return new MiruPartitionResponse<>(
                collaborativeFiltering.collaborativeFiltering(solutionLog, bitmaps, context, request, report, bitmaps.create(), bitmaps.create(),
                    removeDistinctsFilter),
                solutionLog.asList());
        }

        // Start building up list of bitmap operations to run
        List<BM> okAnds = new ArrayList<>();

        // 1) Execute the combined filter above on the given stream, add the bitmap
        BM filtered = bitmaps.create();
        aggregateUtil.filter(bitmaps, context.getSchema(), context.getTermComposer(), context.getFieldIndexProvider(), request.query.scorableFilter,
            solutionLog, filtered, context.getActivityIndex().lastId(), -1);
        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "constrained scorable down to {} items.", bitmaps.cardinality(filtered));
            solutionLog.log(MiruSolutionLogLevel.TRACE, "constrained scorable down bitmap {}", filtered);
        }
        okAnds.add(filtered);

        // 2) Add in the authz check if we have it
        if (!MiruAuthzExpression.NOT_PROVIDED.equals(request.authzExpression)) {
            BM compositeAuthz = context.getAuthzIndex().getCompositeAuthz(request.authzExpression);
            if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
                solutionLog.log(MiruSolutionLogLevel.INFO, "compositeAuthz contains {} items.", bitmaps.cardinality(compositeAuthz));
                solutionLog.log(MiruSolutionLogLevel.TRACE, "compositeAuthz bitmap {}", compositeAuthz);
            }
            okAnds.add(compositeAuthz);
        }

        // 3) Mask out anything that hasn't made it into the activityIndex yet, or that has been removed from the index
        BM buildIndexMask = bitmaps.buildIndexMask(context.getActivityIndex().lastId(), context.getRemovalIndex().getIndex());
        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "indexMask contains {} items.", bitmaps.cardinality(buildIndexMask));
            solutionLog.log(MiruSolutionLogLevel.TRACE, "indexMask bitmap {}", buildIndexMask);
        }
        okAnds.add(buildIndexMask);

        // AND it all together and return the results
        BM okActivity = bitmaps.create();
        bitmapsDebug.debug(solutionLog, bitmaps, "ands", okAnds);
        bitmaps.and(okActivity, okAnds);

        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "answering {} items.", bitmaps.cardinality(okActivity));
            solutionLog.log(MiruSolutionLogLevel.TRACE, "answering bitmap {}", okActivity);
        }

        BM allMyActivity = bitmaps.create();
        aggregateUtil.filter(bitmaps, context.getSchema(), context.getTermComposer(), context.getFieldIndexProvider(), request.query.constraintsFilter,
            solutionLog, allMyActivity, context.getActivityIndex().lastId(), -1);
        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "constrained mine down to {} items.", bitmaps.cardinality(allMyActivity));
            solutionLog.log(MiruSolutionLogLevel.TRACE, "constrained mine down bitmap {}", allMyActivity);
        }

        return new MiruPartitionResponse<>(
            collaborativeFiltering.collaborativeFiltering(solutionLog, bitmaps, context, request, report, allMyActivity, okActivity, removeDistinctsFilter),
            solutionLog.asList());
    }

    @Override
    public MiruPartitionResponse<RecoAnswer> askRemote(HttpClient httpClient,
        MiruPartitionId partitionId,
        Optional<RecoReport> report) throws MiruQueryServiceException {
        return remotePartition.askRemote(httpClient, partitionId, request, report);
    }

    @Override
    public Optional<RecoReport> createReport(Optional<RecoAnswer> answer) {
        Optional<RecoReport> report;
        if (answer.isPresent()) {
            report = Optional.of(new RecoReport(removeDistinctsFilter, answer.get().results.size()));
        } else {
            report = Optional.of(new RecoReport(removeDistinctsFilter, 0));
        }
        return report;
    }
}
