package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.googlecode.javaewah.BitmapStorage;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.query.DistinctCountQuery;
import com.jivesoftware.os.miru.api.query.MiruTimeRange;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.api.query.result.DistinctCountResult;
import com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition;
import com.jivesoftware.os.miru.service.partition.MiruQueryHandle;
import com.jivesoftware.os.miru.service.partition.MiruRemoteHostedPartition;
import com.jivesoftware.os.miru.service.query.DistinctCountReport;
import com.jivesoftware.os.miru.service.query.base.ExecuteMiruFilter;
import com.jivesoftware.os.miru.service.stream.MiruQueryStream;
import java.util.ArrayList;
import java.util.List;

/** @author jonathan */
public class CountCustomExecuteQuery implements ExecuteQuery<DistinctCountResult, DistinctCountReport> {

    private final MiruFilterUtils utils;
    private final DistinctCountQuery query;
    private final int bitsetBufferSize;

    public CountCustomExecuteQuery(MiruFilterUtils utils,
        DistinctCountQuery query,
        int bitsetBufferSize) {
        this.utils = utils;
        this.query = query;
        this.bitsetBufferSize = bitsetBufferSize;
    }

    @Override
    public DistinctCountResult executeLocal(MiruLocalHostedPartition partition, Optional<DistinctCountReport> report) throws Exception {
        try (MiruQueryHandle handle = partition.getQueryHandle()) {

            MiruQueryStream stream = handle.getQueryStream();

            // First grab the stream filter (required)
            MiruFilter combinedFilter = query.streamFilter;

            // If we have a constraints filter grab that as well and AND it to the stream filter
            if (query.constraintsFilter.isPresent()) {
                combinedFilter = new MiruFilter(MiruFilterOperation.and, Optional.<ImmutableList<MiruFieldFilter>>absent(),
                    Optional.of(ImmutableList.of(query.streamFilter, query.constraintsFilter.get())));
            }

            // Start building up list of bitmap operations to run
            List<EWAHCompressedBitmap> ands = new ArrayList<>();

            // 1) Execute the combined filter above on the given stream, add the bitmap
            ExecuteMiruFilter executeMiruFilter = new ExecuteMiruFilter(stream.schema, stream.fieldIndex, stream.executorService,
                combinedFilter, Optional.<BitmapStorage>absent(), -1, bitsetBufferSize);
            ands.add(executeMiruFilter.call());

            // 2) Add in the authz check if we have it
            if (query.authzExpression.isPresent()) {
                ands.add(stream.authzIndex.getCompositeAuthz(query.authzExpression.get()));
            }

            // 3) Add in a time-range mask if we have it
            if (query.timeRange.isPresent()) {
                MiruTimeRange timeRange = query.timeRange.get();
                ands.add(utils.buildTimeRangeMask(stream.timeIndex, timeRange.smallestTimestamp, timeRange.largestTimestamp));
            }

            // 4) Mask out anything that hasn't made it into the activityIndex yet, orToSourceSize that has been removed from the index
            ands.add(utils.buildIndexMask(stream.activityIndex.lastId(), Optional.of(stream.removalIndex.getIndex())));

            // AND it all together and return the results
            EWAHCompressedBitmap answer = utils.bufferedAnd(ands, bitsetBufferSize);
            DistinctCountResult numberOfDistincts = utils.numberOfDistincts(stream, query, report, answer);

            return numberOfDistincts;
        }
    }

    @Override
    public DistinctCountResult executeRemote(MiruRemoteHostedPartition partition, Optional<DistinctCountResult> lastResult) throws Exception {
        MiruReader miruReader = partition.getMiruReader();

        return miruReader.countCustomStream(partition.getPartitionId(), query, lastResult);
    }
}
