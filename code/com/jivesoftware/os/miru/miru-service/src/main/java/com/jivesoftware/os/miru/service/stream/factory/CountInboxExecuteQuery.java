package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.query.DistinctCountQuery;
import com.jivesoftware.os.miru.api.query.MiruTimeRange;
import com.jivesoftware.os.miru.api.query.result.DistinctCountResult;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmaps;
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
public class CountInboxExecuteQuery<BM> implements ExecuteQuery<DistinctCountResult, DistinctCountReport> {

    private final MiruBitmaps<BM> bitmaps;
    private final MiruFilterUtils<BM> utils;
    private final MiruJustInTimeBackfillerizer backfillerizer;
    private final DistinctCountQuery query;
    private final Optional<String> readStreamIdsPropName;
    private final boolean unreadOnly;

    public CountInboxExecuteQuery(MiruBitmaps<BM> bitmaps,
            MiruFilterUtils<BM> utils,
            MiruJustInTimeBackfillerizer backfillerizer,
            DistinctCountQuery query,
            Optional<String> readStreamIdsPropName,
            boolean unreadOnly) {

        Preconditions.checkArgument(query.streamId.isPresent(), "Inbox queries require a streamId");
        this.bitmaps = bitmaps;
        this.utils = utils;
        this.backfillerizer = backfillerizer;
        this.query = query;
        this.readStreamIdsPropName = readStreamIdsPropName;
        this.unreadOnly = unreadOnly;
    }

    @Override
    public DistinctCountResult executeLocal(MiruLocalHostedPartition partition, Optional<DistinctCountReport> report) throws Exception {
        try (MiruQueryHandle handle = partition.getQueryHandle()) {

            MiruQueryStream<BM> stream = handle.getQueryStream();
            if (handle.canBackfill()) {
                backfillerizer.backfill(stream, query.streamFilter, query.tenantId, handle.getPartitionId(), query.streamId.get(), readStreamIdsPropName);
            }

            List<BM> ands = new ArrayList<>();
            if (query.timeRange.isPresent()) {
                MiruTimeRange timeRange = query.timeRange.get();

                // Short-circuit if the time range doesn't live here
                if (!timeIndexIntersectsTimeRange(stream.timeIndex, timeRange)) {
                    return utils.numberOfDistincts(stream, query, report, bitmaps.create());
                }
                ands.add(bitmaps.buildTimeRangeMask(stream.timeIndex, timeRange.smallestTimestamp, timeRange.largestTimestamp));
            }

            Optional<BM> inbox = stream.inboxIndex.getInbox(query.streamId.get());
            if (inbox.isPresent()) {
                ands.add(inbox.get());
            } else {
                // Short-circuit if the user doesn't have an inbox here
                return utils.numberOfDistincts(stream, query, report, bitmaps.create());
            }

            if (query.constraintsFilter.isPresent()) {
                ExecuteMiruFilter<BM> executeMiruFilter = new ExecuteMiruFilter<>(bitmaps, stream.schema, stream.fieldIndex, stream.executorService,
                    query.constraintsFilter.get(), Optional.<BM>absent(), -1);
                ands.add(executeMiruFilter.call());
            }
            if (query.authzExpression.isPresent()) {
                ands.add(stream.authzIndex.getCompositeAuthz(query.authzExpression.get()));
            }
            if (unreadOnly) {
                Optional<BM> unreadIndex = stream.unreadTrackingIndex.getUnread(query.streamId.get());
                if (unreadIndex.isPresent()) {
                    ands.add(unreadIndex.get());
                }
            }
            ands.add(bitmaps.buildIndexMask(stream.activityIndex.lastId(), Optional.of(stream.removalIndex.getIndex())));
            BM answer = utils.bufferedAnd(ands);

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
