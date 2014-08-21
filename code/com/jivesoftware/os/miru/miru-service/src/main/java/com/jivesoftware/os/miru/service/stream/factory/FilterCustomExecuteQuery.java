package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.query.AggregateCountsQuery;
import com.jivesoftware.os.miru.api.query.MiruTimeRange;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition;
import com.jivesoftware.os.miru.service.partition.MiruQueryHandle;
import com.jivesoftware.os.miru.service.partition.MiruRemoteHostedPartition;
import com.jivesoftware.os.miru.service.query.AggregateCountsReport;
import com.jivesoftware.os.miru.service.query.base.ExecuteMiruFilter;
import com.jivesoftware.os.miru.service.stream.MiruQueryStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** @author jonathan */
public class FilterCustomExecuteQuery<BM> implements ExecuteQuery<AggregateCountsResult, AggregateCountsReport> {

    private final MiruBitmaps<BM> bitmaps;
    private final MiruFilterUtils<BM> utils;
    private final AggregateCountsQuery query;

    public FilterCustomExecuteQuery(MiruBitmaps<BM> bitmaps,MiruFilterUtils<BM> utils, AggregateCountsQuery query) {
        this.bitmaps = bitmaps;
        this.utils = utils;
        this.query = query;
    }

    @Override
    public AggregateCountsResult executeLocal(MiruLocalHostedPartition partition, Optional<AggregateCountsReport> report) throws Exception {
        try (MiruQueryHandle handle = partition.getQueryHandle()) {

            MiruQueryStream<BM> stream = handle.getQueryStream();

            // TODO add the concept of a deletion index
            MiruFilter combinedFilter = query.streamFilter;
            if (query.constraintsFilter.isPresent()) {
                combinedFilter = new MiruFilter(MiruFilterOperation.and, Optional.<ImmutableList<MiruFieldFilter>>absent(),
                    Optional.of(ImmutableList.of(query.streamFilter, query.constraintsFilter.get())));
            }

            List<BM> ands = new ArrayList<>();
            ExecuteMiruFilter<BM> executeMiruFilter = new ExecuteMiruFilter<>(bitmaps, stream.schema, stream.fieldIndex, stream.executorService,
                combinedFilter, Optional.<BM>absent(), -1);
            ands.add(executeMiruFilter.call());
            ands.add(bitmaps.buildIndexMask(stream.activityIndex.lastId(), Optional.of(stream.removalIndex.getIndex())));
            if (query.authzExpression.isPresent()) {
                ands.add(stream.authzIndex.getCompositeAuthz(query.authzExpression.get()));
            }
            if (query.answerTimeRange.isPresent()) {
                MiruTimeRange timeRange = query.answerTimeRange.get();
                ands.add(bitmaps.buildTimeRangeMask(stream.timeIndex, timeRange.smallestTimestamp, timeRange.largestTimestamp));
            }
            BM answer = utils.bufferedAnd(ands);

            BM counter = null;
            if (query.countTimeRange.isPresent()) {
                counter = bitmaps.create();
                bitmaps.and(counter, Arrays.asList(answer, bitmaps.buildTimeRangeMask(
                    stream.timeIndex, query.countTimeRange.get().smallestTimestamp, query.countTimeRange.get().largestTimestamp)));
            }

            AggregateCountsResult aggregateCounts = utils.getAggregateCounts(stream, query, report, answer, Optional.fromNullable(counter));

            return aggregateCounts;
        }
    }

    @Override
    public AggregateCountsResult executeRemote(MiruRemoteHostedPartition partition, Optional<AggregateCountsResult> lastResult) throws Exception {
        MiruReader miruReader = partition.getMiruReader();

        return miruReader.filterCustomStream(partition.getPartitionId(), query, lastResult);
    }
}
