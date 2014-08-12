package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.googlecode.javaewah.BitmapStorage;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.query.AggregateCountsQuery;
import com.jivesoftware.os.miru.api.query.MiruTimeRange;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult;
import com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition;
import com.jivesoftware.os.miru.service.partition.MiruQueryHandle;
import com.jivesoftware.os.miru.service.partition.MiruRemoteHostedPartition;
import com.jivesoftware.os.miru.service.query.AggregateCountsReport;
import com.jivesoftware.os.miru.service.query.base.ExecuteMiruFilter;
import com.jivesoftware.os.miru.service.stream.MiruQueryStream;
import java.util.ArrayList;
import java.util.List;

/** @author jonathan */
public class FilterCustomExecuteQuery implements ExecuteQuery<AggregateCountsResult, AggregateCountsReport> {

    private final MiruFilterUtils utils;
    private final AggregateCountsQuery query;
    private final int bitsetBufferSize;

    public FilterCustomExecuteQuery(MiruFilterUtils utils, AggregateCountsQuery query, int bitsetBufferSize) {
        this.utils = utils;
        this.query = query;
        this.bitsetBufferSize = bitsetBufferSize;
    }

    @Override
    public AggregateCountsResult executeLocal(MiruLocalHostedPartition partition, Optional<AggregateCountsReport> report) throws Exception {
        try (MiruQueryHandle handle = partition.getQueryHandle()) {

            MiruQueryStream stream = handle.getQueryStream();

            // TODO add the concept of a deletion index
            MiruFilter combinedFilter = query.streamFilter;
            if (query.constraintsFilter.isPresent()) {
                combinedFilter = new MiruFilter(MiruFilterOperation.and, Optional.<ImmutableList<MiruFieldFilter>>absent(),
                    Optional.of(ImmutableList.of(query.streamFilter, query.constraintsFilter.get())));
            }

            List<EWAHCompressedBitmap> ands = new ArrayList<>();
            ExecuteMiruFilter executeMiruFilter = new ExecuteMiruFilter(stream.schema, stream.fieldIndex, stream.executorService,
                combinedFilter, Optional.<BitmapStorage>absent(), -1, bitsetBufferSize);
            ands.add(executeMiruFilter.call());
            ands.add(utils.buildIndexMask(stream.activityIndex.lastId(), Optional.of(stream.removalIndex.getIndex())));
            if (query.authzExpression.isPresent()) {
                ands.add(stream.authzIndex.getCompositeAuthz(query.authzExpression.get()));
            }
            if (query.answerTimeRange.isPresent()) {
                MiruTimeRange timeRange = query.answerTimeRange.get();
                ands.add(utils.buildTimeRangeMask(stream.timeIndex, timeRange.smallestTimestamp, timeRange.largestTimestamp));
            }
            EWAHCompressedBitmap answer = utils.bufferedAnd(ands, bitsetBufferSize);

            EWAHCompressedBitmap counter = null;
            if (query.countTimeRange.isPresent()) {
                counter = answer.and(utils.buildTimeRangeMask(
                    stream.timeIndex, query.countTimeRange.get().smallestTimestamp, query.countTimeRange.get().largestTimestamp));
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
