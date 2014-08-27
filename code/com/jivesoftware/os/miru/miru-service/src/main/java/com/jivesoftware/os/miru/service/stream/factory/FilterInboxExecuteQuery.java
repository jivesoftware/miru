package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.query.AggregateCountsQuery;
import com.jivesoftware.os.miru.api.query.MiruTimeRange;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult;
import com.jivesoftware.os.miru.reader.MiruHttpClientReader;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.service.index.MiruTimeIndex;
import com.jivesoftware.os.miru.service.partition.MiruQueryHandle;
import com.jivesoftware.os.miru.service.query.AggregateCountsReport;
import com.jivesoftware.os.miru.service.query.base.ExecuteMiruFilter;
import com.jivesoftware.os.miru.service.stream.MiruQueryStream;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jonathan
 */
public class FilterInboxExecuteQuery<BM> implements ExecuteQuery<AggregateCountsResult, AggregateCountsReport> {

    private final MiruBitmaps<BM> bitmaps;
    private final MiruFilterUtils<BM> utils;
    private final MiruJustInTimeBackfillerizer<BM> backfillerizer;
    private final AggregateCountsQuery query;
    private final Optional<String> readStreamIdsPropName;
    private final boolean unreadOnly;

    public FilterInboxExecuteQuery(MiruBitmaps<BM> bitmaps,
            MiruFilterUtils<BM> utils,
            MiruJustInTimeBackfillerizer<BM> backfillerizer,
            AggregateCountsQuery query,
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
    public AggregateCountsResult executeLocal(MiruQueryHandle handle, Optional<AggregateCountsReport> report) throws Exception {
        MiruQueryStream<BM> stream = handle.getQueryStream();

        if (handle.canBackfill()) {
            backfillerizer.backfill(stream, query.streamFilter, query.tenantId, handle.getCoord().partitionId, query.streamId.get(), readStreamIdsPropName);
        }

        List<BM> ands = new ArrayList<>();
        List<BM> counterAnds = new ArrayList<>();

        if (query.answerTimeRange.isPresent()) {
            MiruTimeRange timeRange = query.answerTimeRange.get();

            // Short-circuit if the time range doesn't live here
            if (!timeIndexIntersectsTimeRange(stream.timeIndex, timeRange)) {
                return utils.getAggregateCounts(stream, query, report, bitmaps.create(), Optional.of(bitmaps.create()));
            }
            ands.add(bitmaps.buildTimeRangeMask(stream.timeIndex, timeRange.smallestTimestamp, timeRange.largestTimestamp));
        }
        if (query.countTimeRange.isPresent()) {
            MiruTimeRange timeRange = query.countTimeRange.get();

            // Short-circuit if the time range doesn't live here
            if (!timeIndexIntersectsTimeRange(stream.timeIndex, timeRange)) {
                return utils.getAggregateCounts(stream, query, report, bitmaps.create(), Optional.of(bitmaps.create()));
            }
            counterAnds.add(bitmaps.buildTimeRangeMask(
                    stream.timeIndex, query.countTimeRange.get().smallestTimestamp, query.countTimeRange.get().largestTimestamp));
        }

        Optional<BM> inbox = stream.inboxIndex.getInbox(query.streamId.get());
        if (inbox.isPresent()) {
            ands.add(inbox.get());
        } else {
            // Short-circuit if the user doesn't have an inbox here
            return utils.getAggregateCounts(stream, query, report, bitmaps.create(), Optional.of(bitmaps.create()));
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

        counterAnds.add(answer);
        if (!unreadOnly) {
            // if unreadOnly is true, the read-tracking index would already be applied to the answer
            Optional<BM> unreadIndex = stream.unreadTrackingIndex.getUnread(query.streamId.get());
            if (unreadIndex.isPresent()) {
                counterAnds.add(unreadIndex.get());
            }
        }
        BM counter = utils.bufferedAnd(counterAnds);

        AggregateCountsResult aggregateCounts = utils.getAggregateCounts(stream, query, report, answer, Optional.of(counter));

        return aggregateCounts;
    }

    @Override
    public AggregateCountsResult executeRemote(RequestHelper requestHelper, MiruPartitionId partitionId, Optional<AggregateCountsResult> lastResult)
            throws Exception {
        MiruHttpClientReader reader = new MiruHttpClientReader(requestHelper);
        if (unreadOnly) {
            return reader.filterInboxStreamUnread(partitionId, query, lastResult);
        }
        return reader.filterInboxStreamAll(partitionId, query, lastResult);
    }

    @Override
    public Optional<AggregateCountsReport> createReport(Optional<AggregateCountsResult> result) {
        Optional<AggregateCountsReport> report = Optional.absent();
        if (result.isPresent()) {
            report = Optional.of(new AggregateCountsReport(
                    result.get().skippedDistincts,
                    result.get().collectedDistincts,
                    result.get().aggregateTerms));
        }
        return report;
    }

    private boolean timeIndexIntersectsTimeRange(MiruTimeIndex timeIndex, MiruTimeRange timeRange) {
        return timeRange.smallestTimestamp <= timeIndex.getLargestTimestamp() &&
                timeRange.largestTimestamp >= timeIndex.getSmallestTimestamp();
    }

}
