package com.jivesoftware.os.miru.stream.plugins.count;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.query.CardinalityAndLastSetBit;
import com.jivesoftware.os.miru.query.MiruBitmaps;
import com.jivesoftware.os.miru.query.MiruField;
import com.jivesoftware.os.miru.query.MiruInvertedIndex;
import com.jivesoftware.os.miru.query.MiruQueryStream;
import com.jivesoftware.os.miru.query.ReusableBuffers;
import java.util.Collections;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

/**
 *
 */
public class NumberOfDistincts {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    public <BM> DistinctCountResult numberOfDistincts(MiruBitmaps<BM> bitmaps,
            MiruQueryStream<BM> stream,
            DistinctCountQuery query,
            Optional<DistinctCountReport> lastReport,
            BM answer)
            throws Exception {

        log.debug("Get number of distincts for answer {}", answer);

        int collectedDistincts = 0;
        Set<MiruTermId> aggregateTerms;
        if (lastReport.isPresent()) {
            collectedDistincts = lastReport.get().collectedDistincts;
            aggregateTerms = Sets.newHashSet(lastReport.get().aggregateTerms);
        } else {
            aggregateTerms = Sets.newHashSet();
        }

        int fieldId = stream.schema.getFieldId(query.aggregateCountAroundField);
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
                    MiruTermId aggregateTermId = fieldValues[0];

                    aggregateTerms.add(aggregateTermId);
                    Optional<MiruInvertedIndex<BM>> invertedIndex = aggregateField.getInvertedIndex(aggregateTermId);
                    checkState(invertedIndex.isPresent(), "Unable to load inverted index for aggregateTermId: " + aggregateTermId);

                    BM revisedAnswer = reusable.next();
                    answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, invertedIndex.get().getIndex());
                    answer = revisedAnswer;

                    collectedDistincts++;

                    if (collectedDistincts > query.desiredNumberOfDistincts) {
                        break;
                    }
                }
            }
        }
        return new DistinctCountResult(ImmutableSet.copyOf(aggregateTerms), collectedDistincts);
    }

}
