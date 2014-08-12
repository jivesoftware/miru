package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.googlecode.javaewah.BitmapStorage;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.query.DistinctCountQuery;
import com.jivesoftware.os.miru.api.query.MiruTimeRange;
import com.jivesoftware.os.miru.api.query.result.DistinctCountResult;
import com.jivesoftware.os.miru.service.index.MiruTimeIndex;
import com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition;
import com.jivesoftware.os.miru.service.partition.MiruQueryHandle;
import com.jivesoftware.os.miru.service.partition.MiruRemoteHostedPartition;
import com.jivesoftware.os.miru.service.query.DistinctCountReport;
import com.jivesoftware.os.miru.service.query.base.ExecuteMiruFilter;
import com.jivesoftware.os.miru.service.stream.MiruQueryStream;
import java.util.ArrayList;
import java.util.List;

/** @author jonathan */
public class CountInboxExecuteQuery implements ExecuteQuery<DistinctCountResult, DistinctCountReport> {

    private final MiruFilterUtils utils;
    private final MiruJustInTimeBackfillerizer backfillerizer;
    private final DistinctCountQuery query;
    private final boolean unreadOnly;
    private final int bitsetBufferSize;

    public CountInboxExecuteQuery(MiruFilterUtils utils,
        MiruJustInTimeBackfillerizer backfillerizer,
        DistinctCountQuery query,
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
    public DistinctCountResult executeLocal(MiruLocalHostedPartition partition, Optional<DistinctCountReport> report) throws Exception {
        try (MiruQueryHandle handle = partition.getQueryHandle()) {

            MiruQueryStream stream = handle.getQueryStream();
            if (handle.canBackfill()) {
                backfillerizer.backfill(stream, query.streamFilter, query.tenantId, handle.getPartitionId(), query.streamId.get(), bitsetBufferSize);
            }

            List<EWAHCompressedBitmap> ands = new ArrayList<>();
            if (query.timeRange.isPresent()) {
                MiruTimeRange timeRange = query.timeRange.get();

                // Short-circuit if the time range doesn't live here
                if (!timeIndexIntersectsTimeRange(stream.timeIndex, timeRange)) {
                    return utils.numberOfDistincts(stream, query, report, new EWAHCompressedBitmap());
                }
                ands.add(utils.buildTimeRangeMask(stream.timeIndex, timeRange.smallestTimestamp, timeRange.largestTimestamp));
            }

            Optional<EWAHCompressedBitmap> inbox = stream.inboxIndex.getInbox(query.streamId.get());
            if (inbox.isPresent()) {
                ands.add(inbox.get());
            } else {
                // Short-circuit if the user doesn't have an inbox here
                return utils.numberOfDistincts(stream, query, report, new EWAHCompressedBitmap());
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

            DistinctCountResult numberOfDistincts = utils.numberOfDistincts(stream, query, report, answer);

            return numberOfDistincts;
        }
    }

    @Override
    public DistinctCountResult executeRemote(MiruRemoteHostedPartition partition, Optional<DistinctCountResult> lastResult) throws Exception {
        MiruReader miruReader = partition.getMiruReader();

        if (unreadOnly) {
            return miruReader.countInboxStreamUnread(partition.getPartitionId(), query, lastResult);
        }
        return miruReader.countInboxStreamUnread(partition.getPartitionId(), query, lastResult);
    }

    private boolean timeIndexIntersectsTimeRange(MiruTimeIndex timeIndex, MiruTimeRange timeRange) {
        return timeRange.smallestTimestamp <= timeIndex.getLargestTimestamp() &&
            timeRange.largestTimestamp >= timeIndex.getSmallestTimestamp();
    }
}
