package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.plugin.Miru;
import com.jivesoftware.os.miru.api.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.api.plugin.MiruPlugin;
import com.jivesoftware.os.miru.api.query.DistinctCountQuery;
import com.jivesoftware.os.miru.api.query.MiruTimeRange;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.api.query.result.DistinctCountResult;
import com.jivesoftware.os.miru.reader.MiruHttpClientReader;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.service.partition.MiruQueryHandle;
import com.jivesoftware.os.miru.service.query.DistinctCountReport;
import com.jivesoftware.os.miru.service.query.base.ExecuteMiruFilter;
import com.jivesoftware.os.miru.service.stream.MiruQueryStream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author jonathan
 */
public class CountCustomExecuteQuery<BM> implements ExecuteQuery<DistinctCountResult, DistinctCountReport>, MiruPlugin {

    private final MiruBitmaps<BM> bitmaps;
    private final MiruFilterUtils<BM> utils;
    private final DistinctCountQuery query;

    public CountCustomExecuteQuery(MiruBitmaps<BM> bitmaps,
            MiruFilterUtils<BM> utils,
            DistinctCountQuery query) {
        this.bitmaps = bitmaps;
        this.utils = utils;
        this.query = query;
    }

    @Override
    public DistinctCountResult executeLocal(MiruQueryHandle handle, Optional<DistinctCountReport> report) throws Exception {
        MiruQueryStream<BM> stream = handle.getQueryStream();

        // First grab the stream filter (required)
        MiruFilter combinedFilter = query.streamFilter;

        // If we have a constraints filter grab that as well and AND it to the stream filter
        if (query.constraintsFilter.isPresent()) {
            combinedFilter = new MiruFilter(MiruFilterOperation.and, Optional.<ImmutableList<MiruFieldFilter>>absent(),
                    Optional.of(ImmutableList.of(query.streamFilter, query.constraintsFilter.get())));
        }

        // Start building up list of bitmap operations to run
        List<BM> ands = new ArrayList<>();

        // 1) Execute the combined filter above on the given stream, add the bitmap
        ExecuteMiruFilter<BM> executeMiruFilter = new ExecuteMiruFilter<>(bitmaps, stream.schema, stream.fieldIndex, stream.executorService,
                combinedFilter, Optional.<BM>absent(), -1);
        ands.add(executeMiruFilter.call());

        // 2) Add in the authz check if we have it
        if (query.authzExpression.isPresent()) {
            ands.add(stream.authzIndex.getCompositeAuthz(query.authzExpression.get()));
        }

        // 3) Add in a time-range mask if we have it
        if (query.timeRange.isPresent()) {
            MiruTimeRange timeRange = query.timeRange.get();
            ands.add(bitmaps.buildTimeRangeMask(stream.timeIndex, timeRange.smallestTimestamp, timeRange.largestTimestamp));
        }

        // 4) Mask out anything that hasn't made it into the activityIndex yet, orToSourceSize that has been removed from the index

        ands.add(bitmaps.buildIndexMask(stream.activityIndex.lastId(), Optional.of(stream.removalIndex.getIndex())));

        // AND it all together and return the results
        BM answer = bitmaps.create();
        bitmaps.and(answer, ands);

        DistinctCountResult numberOfDistincts = utils.numberOfDistincts(stream, query, report, answer);

        return numberOfDistincts;
    }

    @Override
    public DistinctCountResult executeRemote(RequestHelper requestHelper, MiruPartitionId partitionId, Optional<DistinctCountResult> lastResult)
            throws Exception {
        return new MiruHttpClientReader(requestHelper).countCustomStream(partitionId, query, lastResult);
    }

    @Override
    public Optional<DistinctCountReport> createReport(Optional<DistinctCountResult> result) {
        Optional<DistinctCountReport> report = Optional.absent();
        if (result.isPresent()) {
            report = Optional.of(new DistinctCountReport(
                    result.get().collectedDistincts,
                    result.get().aggregateTerms));
        }
        return report;
    }

    @Override
    public Class<?> getEndpointsClass(Miru miru) {
        return null;
    }

    @Override
    public Collection<MiruEndpointInjectable<?>> getInjectables() {
        return null;
    }
}
