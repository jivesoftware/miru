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
import com.jivesoftware.os.miru.reco.trending.SimpleRegressionTrend;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.jivesoftware.os.miru.reco.plugins.trending.TrendingResult.Trendy;

/**
 *
 */
public class Trending {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    public <BM> TrendingResult trending(MiruBitmaps<BM> bitmaps,
            MiruQueryStream<BM> stream,
            TrendingQuery query,
            Optional<TrendingReport> lastReport,
            BM answer)
            throws Exception {

        log.debug("Get trending for answer {}", answer);

        int collectedDistincts = 0;
        Set<MiruTermId> aggregateTerms;
        if (lastReport.isPresent()) {
            collectedDistincts = lastReport.get().collectedDistincts;
            aggregateTerms = Sets.newHashSet(lastReport.get().aggregateTerms);
        } else {
            aggregateTerms = Sets.newHashSet();
        }

        List<Trendy> trendies = new ArrayList<>();
        int fieldId = stream.schema.getFieldId(query.aggregateCountAroundField);
        if (fieldId >= 0) {
            MiruField<BM> aggregateField = stream.fieldIndex.getField(fieldId);

            ReusableBuffers<BM> reusable = new ReusableBuffers<>(bitmaps, 2);
            for (MiruTermId aggregateTermId : aggregateTerms) { // Consider
                Optional<MiruInvertedIndex<BM>> invertedIndex = aggregateField.getInvertedIndex(aggregateTermId);
                if (!invertedIndex.isPresent()) {
                    continue;
                }

                BM termIndex = invertedIndex.get().getIndex();
                BM revisedAnswer = reusable.next();
                bitmaps.andNot(revisedAnswer, answer, Collections.<BM>singletonList(termIndex));
                answer = revisedAnswer;

                SimpleRegressionTrend trend = new SimpleRegressionTrend();
                MiruIntIterator iter = bitmaps.intIterator(termIndex);
                while (iter.hasNext()) {
                    int index = iter.next();
                    long timestamp = stream.timeIndex.getTimestamp(index);
                    trend.add(timestamp, 1d);
                }
                trendies.add(new Trendy(aggregateTermId.getBytes(), trend, trend.getRank(trend.getCurrentT())));
            }

            CardinalityAndLastSetBit answerCollector = null;
            while (true) {
                int lastSetBit = answerCollector == null ? bitmaps.lastSetBit(answer) : answerCollector.lastSetBit;
                if (lastSetBit < 0) {
                    break;
                }

                MiruTermId[] fieldValues = stream.activityIndex.get(query.tenantId, lastSetBit, fieldId);
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

                    SimpleRegressionTrend trend = new SimpleRegressionTrend();
                    MiruIntIterator iter = bitmaps.intIterator(termIndex);
                    while (iter.hasNext()) {
                        int index = iter.next();
                        long timestamp = stream.timeIndex.getTimestamp(index);
                        trend.add(timestamp, 1d);
                    }
                    Trendy trendy = new Trendy(aggregateValue, trend, trend.getRank(trend.getCurrentT()));
                    trendies.add(trendy);

                    //TODO desiredNumberOfDistincts is used to truncate the final list. Do we need a maxDistincts of some sort?
                    /*
                     if (trendies.size() >= query.desiredNumberOfDistincts) {
                     break;
                     }
                     */
                }
            }
        }

        return new TrendingResult(ImmutableList.copyOf(trendies), ImmutableSet.copyOf(aggregateTerms), collectedDistincts);
    }

}
