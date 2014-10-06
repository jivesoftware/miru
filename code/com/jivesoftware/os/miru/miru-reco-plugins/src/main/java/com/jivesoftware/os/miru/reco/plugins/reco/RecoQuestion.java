package com.jivesoftware.os.miru.reco.plugins.reco;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.Question;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class RecoQuestion implements Question<RecoAnswer, RecoReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final CollaborativeFiltering collaborativeFiltering;
    private final MiruRequest<RecoQuery> request;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public RecoQuestion(CollaborativeFiltering collaborativeFiltering,
            MiruRequest<RecoQuery> query) {
        this.collaborativeFiltering = collaborativeFiltering;
        this.request = query;
    }

    @Override
    public <BM> MiruPartitionResponse<RecoAnswer> askLocal(MiruRequestHandle<BM> handle, Optional<RecoReport> report) throws Exception {
        MiruSolutionLog solutionLog = new MiruSolutionLog(request.debug);

        MiruRequestContext<BM> stream = handle.getRequestContext();
        MiruBitmaps<BM> bitmaps = handle.getBitmaps();

        // Start building up list of bitmap operations to run
        List<BM> ands = new ArrayList<>();

        // 1) Execute the combined filter above on the given stream, add the bitmap
        BM filtered = bitmaps.create();
        aggregateUtil.filter(bitmaps, stream.schema, stream.fieldIndex, request.query.constraintsFilter, filtered, -1);
        if (solutionLog.isEnabled()) {
            solutionLog.log("constrained down to {} items.", bitmaps.cardinality(filtered));
        }
        ands.add(filtered);

        // 2) Add in the authz check if we have it
        if (!MiruAuthzExpression.NOT_PROVIDED.equals(request.authzExpression)) {
            BM compositeAuthz = stream.authzIndex.getCompositeAuthz(request.authzExpression);
            if (solutionLog.isEnabled()) {
                solutionLog.log("compositeAuthz contains {} items.", bitmaps.cardinality(compositeAuthz));
            }
            ands.add(compositeAuthz);
        }

        // 3) Mask out anything that hasn't made it into the activityIndex yet, or that has been removed from the index
        BM buildIndexMask = bitmaps.buildIndexMask(stream.activityIndex.lastId(), Optional.of(stream.removalIndex.getIndex()));
        if (solutionLog.isEnabled()) {
            solutionLog.log("indexMask contains {} items.", bitmaps.cardinality(buildIndexMask));
        }
        ands.add(buildIndexMask);

        // AND it all together and return the results
        BM answer = bitmaps.create();
        bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
        bitmaps.and(answer, ands);

        if (solutionLog.isEnabled()) {
            solutionLog.log("considering {} items.", bitmaps.cardinality(answer));
        }

        return new MiruPartitionResponse<>(
                collaborativeFiltering.collaborativeFiltering(solutionLog, bitmaps, stream, request, report, answer),
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
            report = Optional.of(new RecoReport(answer.get().results.size()));
        }
        return report;
    }
}
