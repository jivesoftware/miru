package com.jivesoftware.os.miru.stream.plugins.count;

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
import com.jivesoftware.os.miru.query.ExecuteMiruFilter;
import com.jivesoftware.os.miru.query.MiruBitmaps;
import com.jivesoftware.os.miru.query.MiruBitmapsDebug;
import com.jivesoftware.os.miru.query.MiruQueryHandle;
import com.jivesoftware.os.miru.query.MiruQueryStream;
import com.jivesoftware.os.miru.query.MiruTimeRange;
import com.jivesoftware.os.miru.query.Question;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jonathan
 */
public class CountCustomQuestion implements Question<DistinctCountAnswer, DistinctCountReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final NumberOfDistincts numberOfDistincts;
    private final DistinctCountQuery query;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();

    public CountCustomQuestion(NumberOfDistincts numberOfDistincts,
            DistinctCountQuery query) {
        this.numberOfDistincts = numberOfDistincts;
        this.query = query;
    }

    @Override
    public <BM> DistinctCountAnswer askLocal(MiruQueryHandle<BM> handle, Optional<DistinctCountReport> report) throws Exception {
        MiruQueryStream<BM> stream = handle.getQueryStream();
        MiruBitmaps<BM> bitmaps = handle.getBitmaps();

        // First grab the stream filter (required)
        MiruFilter combinedFilter = query.streamFilter;

        // If we have a constraints filter grab that as well and AND it to the stream filter
        if (!MiruFilter.NO_FILTER.equals(query.constraintsFilter)) {
            combinedFilter = new MiruFilter(MiruFilterOperation.and, Optional.<ImmutableList<MiruFieldFilter>>absent(),
                    Optional.of(ImmutableList.of(query.streamFilter, query.constraintsFilter)));
        }

        // Start building up list of bitmap operations to run
        List<BM> ands = new ArrayList<>();

        // 1) Execute the combined filter above on the given stream, add the bitmap
        ExecuteMiruFilter<BM> executeMiruFilter = new ExecuteMiruFilter<>(bitmaps, stream.schema, stream.fieldIndex, stream.executorService,
                combinedFilter, Optional.<BM>absent(), -1);
        ands.add(executeMiruFilter.call());

        // 2) Add in the authz check if we have it
        if (!MiruAuthzExpression.NOT_PROVIDED.equals(query.authzExpression)) {
            ands.add(stream.authzIndex.getCompositeAuthz(query.authzExpression));
        }

        // 3) Add in a time-range mask if we have it
        if (!MiruTimeRange.ALL_TIME.equals(query.timeRange)) {
            MiruTimeRange timeRange = query.timeRange;
            ands.add(bitmaps.buildTimeRangeMask(stream.timeIndex, timeRange.smallestTimestamp, timeRange.largestTimestamp));
        }

        // 4) Mask out anything that hasn't made it into the activityIndex yet, orToSourceSize that has been removed from the index
        ands.add(bitmaps.buildIndexMask(stream.activityIndex.lastId(), Optional.of(stream.removalIndex.getIndex())));

        // AND it all together and return the results
        BM answer = bitmaps.create();
        bitmapsDebug.debug(LOG, bitmaps, "ands", ands);
        bitmaps.and(answer, ands);

        return numberOfDistincts.numberOfDistincts(bitmaps, stream, query, report, answer);
    }

    @Override
    public DistinctCountAnswer askRemote(RequestHelper requestHelper, MiruPartitionId partitionId, Optional<DistinctCountAnswer> lastAnswer)
            throws Exception {
        return new DistinctCountRemotePartitionReader(requestHelper).countCustomStream(partitionId, query, lastAnswer);
    }

    @Override
    public Optional<DistinctCountReport> createReport(Optional<DistinctCountAnswer> answer) {
        Optional<DistinctCountReport> report = Optional.absent();
        if (answer.isPresent()) {
            report = Optional.of(new DistinctCountReport(
                    answer.get().collectedDistincts,
                    answer.get().aggregateTerms));
        }
        return report;
    }

}
