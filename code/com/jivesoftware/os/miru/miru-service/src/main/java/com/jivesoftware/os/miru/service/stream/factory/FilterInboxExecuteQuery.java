package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.googlecode.javaewah.BitmapStorage;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.query.AggregateCountsQuery;
import com.jivesoftware.os.miru.api.query.MiruTimeRange;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult;
import com.jivesoftware.os.miru.service.index.MiruTimeIndex;
import com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition;
import com.jivesoftware.os.miru.service.partition.MiruQueryHandle;
import com.jivesoftware.os.miru.service.partition.MiruRemoteHostedPartition;
import com.jivesoftware.os.miru.service.query.AggregateCountsReport;
import com.jivesoftware.os.miru.service.query.base.ExecuteMiruFilter;
import com.jivesoftware.os.miru.service.stream.MiruQueryStream;
import java.util.ArrayList;
import java.util.List;

/** @author jonathan */
public class FilterInboxExecuteQuery implements ExecuteQuery<AggregateCountsResult, AggregateCountsReport> {

    private final MiruFilterUtils utils;
    private final MiruJustInTimeBackfillerizer backfillerizer;
    private final AggregateCountsQuery query;
    private final boolean unreadOnly;
    private final int bitsetBufferSize;

    public FilterInboxExecuteQuery(MiruFilterUtils utils,
        MiruJustInTimeBackfillerizer backfillerizer,
        AggregateCountsQuery query,
        boolean unreadOnly,
        int bitsetBufferSize) {
        Preconditions.checkArgument(query.streamId.isPresent(), "Inbox queries require a streamId");
        this.utils = utils;
        this.backfillerizer = backfillerizer;
        this.query = query;
        this.unreadOnly = unreadOnly;
        this.bitsetBufferSize = bitsetBufferSize;
    }

    @Override
    public AggregateCountsResult executeLocal(MiruLocalHostedPartition partition, Optional<AggregateCountsReport> report) throws Exception {
        try (MiruQueryHandle handle = partition.getQueryHandle()) {

            MiruQueryStream stream = handle.getQueryStream();

            if (handle.canBackfill()) {
                backfillerizer.backfill(stream, query.streamFilter, query.tenantId, handle.getPartitionId(), query.streamId.get(), bitsetBufferSize);
            }

            List<EWAHCompressedBitmap> ands = new ArrayList<>();
            List<EWAHCompressedBitmap> counterAnds = new ArrayList<>();

            if (query.answerTimeRange.isPresent()) {
                MiruTimeRange timeRange = query.answerTimeRange.get();

                // Short-circuit if the time range doesn't live here
                if (!timeIndexIntersectsTimeRange(stream.timeIndex, timeRange)) {
                    return utils.getAggregateCounts(stream, query, report, new EWAHCompressedBitmap(), Optional.of(new EWAHCompressedBitmap()));
                }
                ands.add(utils.buildTimeRangeMask(stream.timeIndex, timeRange.smallestTimestamp, timeRange.largestTimestamp));
            }
            if (query.countTimeRange.isPresent()) {
                MiruTimeRange timeRange = query.countTimeRange.get();

                // Short-circuit if the time range doesn't live here
                if (!timeIndexIntersectsTimeRange(stream.timeIndex, timeRange)) {
                    return utils.getAggregateCounts(stream, query, report, new EWAHCompressedBitmap(), Optional.of(new EWAHCompressedBitmap()));
                }
                counterAnds.add(utils.buildTimeRangeMask(
                    stream.timeIndex, query.countTimeRange.get().smallestTimestamp, query.countTimeRange.get().largestTimestamp));
            }

            Optional<EWAHCompressedBitmap> inbox = stream.inboxIndex.getInbox(query.streamId.get());
            if (inbox.isPresent()) {
                ands.add(inbox.get());
            } else {
                // Short-circuit if the user doesn't have an inbox here
                return utils.getAggregateCounts(stream, query, report, new EWAHCompressedBitmap(), Optional.of(new EWAHCompressedBitmap()));
            }

            if (query.constraintsFilter.isPresent()) {
                ExecuteMiruFilter executeMiruFilter = new ExecuteMiruFilter(stream.schema, stream.fieldIndex, stream.executorService,
                    query.constraintsFilter.get(), Optional.<BitmapStorage>absent(), -1, bitsetBufferSize);
                ands.add(executeMiruFilter.call());
            }
            if (query.authzExpression.isPresent()) {
                ands.add(stream.authzIndex.getCompositeAuthz(query.authzExpression.get()));
            }
            if (unreadOnly) {
                Optional<EWAHCompressedBitmap> unreadIndex = stream.unreadTrackingIndex.getUnread(query.streamId.get());
                if (unreadIndex.isPresent()) {
                    ands.add(unreadIndex.get());
                }
            }
            ands.add(utils.buildIndexMask(stream.activityIndex.lastId(), Optional.of(stream.removalIndex.getIndex())));
            EWAHCompressedBitmap answer = utils.bufferedAnd(ands, bitsetBufferSize);

            counterAnds.add(answer);
            if (!unreadOnly) {
                // if unreadOnly is true, the read-tracking index would already be applied to the answer
                Optional<EWAHCompressedBitmap> unreadIndex = stream.unreadTrackingIndex.getUnread(query.streamId.get());
                if (unreadIndex.isPresent()) {
                    counterAnds.add(unreadIndex.get());
                }
            }
            EWAHCompressedBitmap counter = utils.bufferedAnd(counterAnds, bitsetBufferSize);

            AggregateCountsResult aggregateCounts = utils.getAggregateCounts(stream, query, report, answer, Optional.of(counter));

            return aggregateCounts;
        }
    }

    @Override
    public AggregateCountsResult executeRemote(MiruRemoteHostedPartition partition, Optional<AggregateCountsResult> lastResult) throws Exception {
        MiruReader miruReader = partition.getMiruReader();

        if (unreadOnly) {
            return miruReader.filterInboxStreamUnread(partition.getPartitionId(), query, lastResult);
        }
        return miruReader.filterInboxStreamAll(partition.getPartitionId(), query, lastResult);
    }

    private boolean timeIndexIntersectsTimeRange(MiruTimeIndex timeIndex, MiruTimeRange timeRange) {
        return timeRange.smallestTimestamp <= timeIndex.getLargestTimestamp() &&
            timeRange.largestTimestamp >= timeIndex.getSmallestTimestamp();
    }

}
