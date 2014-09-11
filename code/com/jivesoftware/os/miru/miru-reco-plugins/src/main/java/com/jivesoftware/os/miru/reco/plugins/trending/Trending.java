package com.jivesoftware.os.miru.reco.plugins.trending;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.query.CardinalityAndLastSetBit;
import com.jivesoftware.os.miru.query.MiruBitmaps;
import com.jivesoftware.os.miru.query.MiruField;
import com.jivesoftware.os.miru.query.MiruIntIterator;
import com.jivesoftware.os.miru.query.MiruInvertedIndex;
import com.jivesoftware.os.miru.query.MiruQueryStream;
import com.jivesoftware.os.miru.query.ReusableBuffers;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingAnswer.Trendy;
import com.jivesoftware.os.miru.reco.trending.SimpleRegressionTrend;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

/**
 *
 */
public class Trending {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    public <BM> TrendingAnswer trending(MiruBitmaps<BM> bitmaps,
        MiruQueryStream<BM> stream,
        TrendingQuery query,
        Optional<TrendingReport> lastReport,
        BM answer)
        throws Exception {

        log.debug("Get trending for answer={} query={}", answer, query);

        int collectedDistincts = 0;
        Set<MiruTermId> aggregateTerms;
        if (lastReport.isPresent()) {
            collectedDistincts = lastReport.get().collectedDistincts;
            aggregateTerms = Sets.newHashSet(lastReport.get().aggregateTerms);
        } else {
            aggregateTerms = Sets.newHashSet();
        }

        List<Trendy> trendies = new ArrayList<>();
        final long trendInterval = query.timeRange.largestTimestamp - query.timeRange.smallestTimestamp;
        int fieldId = stream.schema.getFieldId(query.aggregateCountAroundField);
        log.debug("fieldId={}", fieldId);
        if (fieldId >= 0) {
            MiruField<BM> aggregateField = stream.fieldIndex.getField(fieldId);

            ReusableBuffers<BM> reusable = new ReusableBuffers<>(bitmaps, 2);
            for (MiruTermId aggregateTermId : aggregateTerms) {
                Optional<MiruInvertedIndex<BM>> invertedIndex = aggregateField.getInvertedIndex(aggregateTermId);
                if (!invertedIndex.isPresent()) {
                    continue;
                }

                BM termIndex = invertedIndex.get().getIndex();
                BM revisedAnswer = reusable.next();
                bitmaps.andNot(revisedAnswer, answer, Collections.singletonList(termIndex));
                answer = revisedAnswer;

                SimpleRegressionTrend trend = new SimpleRegressionTrend(query.divideTimeRangeIntoNSegments, trendInterval);
                MiruIntIterator iter = bitmaps.intIterator(termIndex);
                while (iter.hasNext()) {
                    int index = iter.next();
                    long timestamp = stream.timeIndex.getTimestamp(index);
                    trend.add(timestamp, 1d);
                }
                trendies.add(new Trendy(aggregateTermId.getBytes(), trend, trend.getRank(trend.getCurrentT())));
            }

            CardinalityAndLastSetBit answerCollector = null;
            int priorLastSetBit = Integer.MAX_VALUE;
            while (true) {
                int lastSetBit = answerCollector == null ? bitmaps.lastSetBit(answer) : answerCollector.lastSetBit;
                log.trace("lastSetBit={}", lastSetBit);
                if (priorLastSetBit <= lastSetBit) {
                    log.error("Failed to make forward progress removing lastSetBit:{} answer:{}", lastSetBit, answer);
                    break;
                }
                priorLastSetBit = lastSetBit;
                if (lastSetBit < 0) {
                    break;
                }

                MiruTermId[] fieldValues = stream.activityIndex.get(query.tenantId, lastSetBit, fieldId);
                log.trace("fieldValues={}", (Object) fieldValues);
                if (fieldValues == null || fieldValues.length == 0) {
                    // could make this a reusable buffer, but this is effectively an error case and would require 3 buffers
                    BM removeUnknownField = bitmaps.create();
                    bitmaps.set(removeUnknownField, lastSetBit);
                    BM revisedAnswer = reusable.next();
                    answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, removeUnknownField);
                    answer = revisedAnswer;

                } else {
                    MiruTermId aggregateTermId = fieldValues[0]; // Kinda lame but for now we don't see a need for multi field aggregation.
                    byte[] aggregateValue = aggregateTermId.getBytes();
                    aggregateTerms.add(aggregateTermId);

                    Optional<MiruInvertedIndex<BM>> invertedIndex = aggregateField.getInvertedIndex(aggregateTermId);
                    checkState(invertedIndex.isPresent(), "Unable to load inverted index for aggregateTermId: " + aggregateTermId);

                    BM termIndex = invertedIndex.get().getIndex();

                    BM revisedAnswer = reusable.next();
                    answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, termIndex);
                    answer = revisedAnswer;

                    collectedDistincts++;

                    SimpleRegressionTrend trend = new SimpleRegressionTrend(query.divideTimeRangeIntoNSegments, trendInterval);
                    MiruIntIterator iter = bitmaps.intIterator(termIndex);
                    long minTime = Long.MAX_VALUE;
                    long maxTime = Long.MIN_VALUE;
                    while (iter.hasNext()) {
                        int index = iter.next();
                        long timestamp = stream.timeIndex.getTimestamp(index);
                        if (timestamp < minTime) {
                            minTime = timestamp;
                        }
                        if (timestamp > maxTime) {
                            maxTime = timestamp;
                        }
                        trend.add(timestamp, 1d);
                    }
                    Trendy trendy = new Trendy(aggregateValue, trend, trend.getRank(query.timeRange.largestTimestamp));
                    trendies.add(trendy);
                    if (log.isTraceEnabled()) {
                        log.trace("Trend minTime={} maxTime={} currentT={} bucketTimes={}", minTime, maxTime, trend.getCurrentT(), trend.getBucketsT());
                    }
                }
            }
        }

        boolean resultsExhausted = query.timeRange.smallestTimestamp >= stream.timeIndex.getSmallestTimestamp();
        TrendingAnswer result = new TrendingAnswer(ImmutableList.copyOf(trendies), ImmutableSet.copyOf(aggregateTerms), collectedDistincts, resultsExhausted);
        log.debug("result={}", result);
        return result;
    }

}
