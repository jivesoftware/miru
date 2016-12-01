package com.jivesoftware.os.miru.reco.plugins.reco;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
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

/**
 *
 */
public class RecoQuestion implements Question<RecoQuery, RecoAnswer, RecoReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final CollaborativeFiltering collaborativeFiltering;
    private final MiruRequest<RecoQuery> request;
    private final MiruRemotePartition<RecoQuery, RecoAnswer, RecoReport> remotePartition;
    private final List<MiruValue> removeDistincts;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public RecoQuestion(CollaborativeFiltering collaborativeFiltering,
        MiruRequest<RecoQuery> query,
        MiruRemotePartition<RecoQuery, RecoAnswer, RecoReport> remotePartition,
        List<MiruValue> removeDistincts) {
        this.collaborativeFiltering = Preconditions.checkNotNull(collaborativeFiltering);
        this.request = Preconditions.checkNotNull(query);
        this.remotePartition = remotePartition;
        this.removeDistincts = Preconditions.checkNotNull(removeDistincts);
    }

    @Override
    public <BM extends IBM, IBM> MiruPartitionResponse<RecoAnswer> askLocal(MiruRequestHandle<BM, IBM, ?> handle,
        Optional<RecoReport> report) throws Exception {

        StackBuffer stackBuffer = new StackBuffer();
        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);

        MiruRequestContext<BM, IBM, ?> context = handle.getRequestContext();
        MiruBitmaps<BM, IBM> bitmaps = handle.getBitmaps();
        MiruTermComposer termComposer = context.getTermComposer();
        MiruSchema schema = context.getSchema();

        MiruTimeRange timeRange = request.query.timeRange;
        if (!context.getTimeIndex().intersects(timeRange)) {
            solutionLog.log(MiruSolutionLogLevel.WARN, "No time index intersection. Partition {}: {} doesn't intersect with {}",
                handle.getCoord().partitionId, context.getTimeIndex(), timeRange);
            return new MiruPartitionResponse<>(
                collaborativeFiltering.collaborativeFiltering("reco",
                    solutionLog,
                    bitmaps,
                    handle.getTrackError(),
                    context,
                    handle.getCoord(),
                    request,
                    report,
                    bitmaps.create(),
                    bitmaps.create(),
                    removeDistincts),
                solutionLog.asList());
        }

        // Start building up list of bitmap operations to run
        List<IBM> okAnds = new ArrayList<>();
        int lastId = context.getActivityIndex().lastId(stackBuffer);

        // 1) Execute the combined filter above on the given stream, add the bitmap
        BM filtered = aggregateUtil.filter("reco", bitmaps, context, request.query.scorableFilter, solutionLog, null, lastId, -1, -1, stackBuffer);
        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "constrained scorable down to {} items.", bitmaps.cardinality(filtered));
            solutionLog.log(MiruSolutionLogLevel.TRACE, "constrained scorable down bitmap {}", filtered);
        }
        okAnds.add(filtered);

        // 2) Add in the authz check if we have it
        if (!MiruAuthzExpression.NOT_PROVIDED.equals(request.authzExpression)) {
            IBM compositeAuthz = context.getAuthzIndex().getCompositeAuthz(request.authzExpression, stackBuffer);
            if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
                solutionLog.log(MiruSolutionLogLevel.INFO, "compositeAuthz contains {} items.", bitmaps.cardinality(compositeAuthz));
                solutionLog.log(MiruSolutionLogLevel.TRACE, "compositeAuthz bitmap {}", compositeAuthz);
            }
            okAnds.add(compositeAuthz);
        }

        // 3) Mask out anything that hasn't made it into the activityIndex yet, or that has been removed from the index
        IBM indexMask = bitmaps.buildIndexMask(lastId, context.getRemovalIndex(), null, stackBuffer);
        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "indexMask contains {} items.", bitmaps.cardinality(indexMask));
            solutionLog.log(MiruSolutionLogLevel.TRACE, "indexMask bitmap {}", indexMask);
        }
        okAnds.add(indexMask);

        // AND it all together and return the results
        bitmapsDebug.debug(solutionLog, bitmaps, "ands", okAnds);
        BM okActivity = bitmaps.and(okAnds);

        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "answering {} items.", bitmaps.cardinality(okActivity));
            solutionLog.log(MiruSolutionLogLevel.TRACE, "answering bitmap {}", okActivity);
        }

        BM allMyActivity = aggregateUtil.filter("reco", bitmaps, context, request.query.constraintsFilter, solutionLog, null, lastId, -1, -1, stackBuffer);
        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "constrained mine down to {} items.", bitmaps.cardinality(allMyActivity));
            solutionLog.log(MiruSolutionLogLevel.TRACE, "constrained mine down bitmap {}", allMyActivity);
        }

        return new MiruPartitionResponse<>(
            collaborativeFiltering.collaborativeFiltering("reco",
                solutionLog,
                bitmaps,
                handle.getTrackError(),
                context,
                handle.getCoord(),
                request,
                report,
                allMyActivity,
                okActivity,
                removeDistincts),
            solutionLog.asList());
    }

    @Override
    public MiruPartitionResponse<RecoAnswer> askRemote(MiruHost host,
        MiruPartitionId partitionId,
        Optional<RecoReport> report) throws MiruQueryServiceException {
        return remotePartition.askRemote(host, partitionId, request, report);
    }

    @Override
    public Optional<RecoReport> createReport(Optional<RecoAnswer> answer) {
        Optional<RecoReport> report;
        if (answer.isPresent()) {
            report = Optional.of(new RecoReport(removeDistincts, answer.get().results.size()));
        } else {
            report = Optional.of(new RecoReport(removeDistincts, 0));
        }
        return report;
    }
}
