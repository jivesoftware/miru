package com.jivesoftware.os.miru.reco.plugins.reco;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.Question;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class RecoQuestion implements Question<RecoAnswer, RecoReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final CollaborativeFiltering collaborativeFiltering;
    private final MiruRequest<RecoQuery> request;
    private final MiruFilter removeDistinctsFilter;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public RecoQuestion(CollaborativeFiltering collaborativeFiltering,
        MiruRequest<RecoQuery> query,
        MiruFilter removeDistinctsFilter) {
        this.collaborativeFiltering = Preconditions.checkNotNull(collaborativeFiltering);
        this.request = Preconditions.checkNotNull(query);
        this.removeDistinctsFilter = Preconditions.checkNotNull(removeDistinctsFilter);
    }

    @Override
    public <BM> MiruPartitionResponse<RecoAnswer> askLocal(MiruRequestHandle<BM> handle, Optional<RecoReport> report) throws Exception {
        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);

        MiruRequestContext<BM> context = handle.getRequestContext();
        MiruBitmaps<BM> bitmaps = handle.getBitmaps();

        // Start building up list of bitmap operations to run
        List<BM> ands = new ArrayList<>();

        // 1) Execute the combined filter above on the given stream, add the bitmap
        BM filtered = bitmaps.create();
        aggregateUtil.filter(bitmaps, context.getSchema(), context.getTermComposer(), context.getFieldIndexProvider(), request.query.constraintsFilter,
            solutionLog, filtered, context.getActivityIndex().lastId(), -1);
        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "constrained down to {} items.", bitmaps.cardinality(filtered));
            solutionLog.log(MiruSolutionLogLevel.TRACE, "constrained down bitmap {}", filtered);
        }
        ands.add(filtered);

        // 2) Add in the authz check if we have it
        if (!MiruAuthzExpression.NOT_PROVIDED.equals(request.authzExpression)) {
            BM compositeAuthz = context.getAuthzIndex().getCompositeAuthz(request.authzExpression);
            if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
                solutionLog.log(MiruSolutionLogLevel.INFO, "compositeAuthz contains {} items.", bitmaps.cardinality(compositeAuthz));
                solutionLog.log(MiruSolutionLogLevel.TRACE, "compositeAuthz bitmap {}", compositeAuthz);
            }
            ands.add(compositeAuthz);
        }

        // 3) Mask out anything that hasn't made it into the activityIndex yet, or that has been removed from the index
        BM buildIndexMask = bitmaps.buildIndexMask(context.getActivityIndex().lastId(), context.getRemovalIndex().getIndex());
        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "indexMask contains {} items.", bitmaps.cardinality(buildIndexMask));
            solutionLog.log(MiruSolutionLogLevel.TRACE, "indexMask bitmap {}", buildIndexMask);
        }
        ands.add(buildIndexMask);

        // AND it all together and return the results
        BM answer = bitmaps.create();
        bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
        bitmaps.and(answer, ands);



        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "considering {} items.", bitmaps.cardinality(answer));
            solutionLog.log(MiruSolutionLogLevel.TRACE, "considering bitmap {}", answer);
        }

        return new MiruPartitionResponse<>(
            collaborativeFiltering.collaborativeFiltering(solutionLog, bitmaps, context, request, report, answer, removeDistinctsFilter),
            solutionLog.asList()
        );
    }

    @Override
    public MiruPartitionResponse<RecoAnswer> askRemote(RequestHelper requestHelper, MiruPartitionId partitionId, Optional<RecoReport> report) throws Exception {
        return new RecoRemotePartitionReader(requestHelper).collaborativeFilteringRecommendations(partitionId, request, report);
    }

    @Override
    public Optional<RecoReport> createReport(Optional<RecoAnswer> answer) {
        Optional<RecoReport> report = Optional.absent();
        if (answer.isPresent()) {
            report = Optional.of(new RecoReport(removeDistinctsFilter, answer.get().results.size()));
        }
        return report;
    }
}
