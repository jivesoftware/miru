package com.jivesoftware.os.miru.reco.plugins.reco;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.query.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.query.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.query.context.MiruRequestContext;
import com.jivesoftware.os.miru.query.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.query.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.query.solution.Question;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class RecoQuestion implements Question<RecoAnswer, RecoReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final CollaborativeFiltering collaborativeFiltering;
    private final RecoQuery query;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public RecoQuestion(CollaborativeFiltering collaborativeFiltering,
            RecoQuery query) {
        this.collaborativeFiltering = collaborativeFiltering;
        this.query = query;
    }

    @Override
    public <BM> RecoAnswer askLocal(MiruRequestHandle<BM> handle, Optional<RecoReport> report) throws Exception {
        MiruRequestContext<BM> stream = handle.getRequestContext();
        MiruBitmaps<BM> bitmaps = handle.getBitmaps();

        // Start building up list of bitmap operations to run
        List<BM> ands = new ArrayList<>();

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

        return collaborativeFiltering.collaborativeFiltering(bitmaps, stream, query, report, answer);
    }

    @Override
    public RecoAnswer askRemote(RequestHelper requestHelper, MiruPartitionId partitionId, Optional<RecoReport> report) throws Exception {
        return new RecoRemotePartitionReader(requestHelper).collaborativeFilteringRecommendations(partitionId, query, report);
    }

    @Override
    public Optional<RecoReport> createReport(Optional<RecoAnswer> answer) {
        Optional<RecoReport> report = Optional.absent();
        if (answer.isPresent()) {
            report = Optional.of(new RecoReport());
        }
        return report;
    }
}
