package com.jivesoftware.os.miru.stream.plugins.count;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.plugin.bitmap.CardinalityAndLastSetBit;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.ReusableBuffers;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

/**
 *
 */
public class DistinctCount {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    public <BM> DistinctCountAnswer numberOfDistincts(MiruBitmaps<BM> bitmaps,
        MiruRequestContext<BM, ?> requestContext,
        MiruRequest<DistinctCountQuery> request,
        Optional<DistinctCountReport> lastReport,
        BM answer)
        throws Exception {

        byte[] primitiveBuffer = new byte[8];

        log.debug("Get number of distincts for answer={} query={}", answer, request);

        int collectedDistincts = 0;
        Set<String> aggregateTerms;
        if (lastReport.isPresent()) {
            collectedDistincts = lastReport.get().collectedDistincts;
            aggregateTerms = Sets.newHashSet(lastReport.get().aggregateTerms);
        } else {
            aggregateTerms = Sets.newHashSet();
        }

        MiruTermComposer termComposer = requestContext.getTermComposer();
        int fieldId = requestContext.getSchema().getFieldId(request.query.aggregateCountAroundField);
        MiruFieldDefinition fieldDefinition = requestContext.getSchema().getFieldDefinition(fieldId);
        log.debug("fieldId={}", fieldId);
        if (fieldId >= 0) {
            MiruFieldIndex<BM> fieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);
            ReusableBuffers<BM> reusable = new ReusableBuffers<>(bitmaps, 2);

            for (String aggregateTerm : aggregateTerms) {
                MiruTermId aggregateTermId = termComposer.compose(fieldDefinition, aggregateTerm);
                Optional<BM> optionalTermIndex = fieldIndex.get(fieldId, aggregateTermId).getIndex(primitiveBuffer);
                if (!optionalTermIndex.isPresent()) {
                    continue;
                }

                BM termIndex = optionalTermIndex.get();

                BM revisedAnswer = reusable.next();
                bitmaps.andNot(revisedAnswer, answer, termIndex);
                answer = revisedAnswer;
            }

            CardinalityAndLastSetBit answerCollector = null;
            while (true) {
                int lastSetBit = answerCollector == null ? bitmaps.lastSetBit(answer) : answerCollector.lastSetBit;
                log.trace("lastSetBit={}", lastSetBit);
                if (lastSetBit < 0) {
                    break;
                }
                MiruTermId[] fieldValues = requestContext.getActivityIndex().get(lastSetBit, fieldId,primitiveBuffer);
                log.trace("fieldValues={}", (Object) fieldValues);
                if (fieldValues == null || fieldValues.length == 0) {
                    // could make this a reusable buffer, but this is effectively an error case and would require 3 buffers
                    BM removeUnknownField = bitmaps.createWithBits(lastSetBit);
                    BM revisedAnswer = reusable.next();
                    answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, removeUnknownField);
                    answer = revisedAnswer;

                } else {
                    MiruTermId aggregateTermId = fieldValues[0];
                    String aggregateTerm = termComposer.decompose(fieldDefinition, aggregateTermId);

                    aggregateTerms.add(aggregateTerm);
                    Optional<BM> optionalTermIndex = fieldIndex.get(fieldId, aggregateTermId).getIndex(primitiveBuffer);
                    checkState(optionalTermIndex.isPresent(), "Unable to load inverted index for aggregateTermId: " + aggregateTermId);

                    BM revisedAnswer = reusable.next();
                    answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, optionalTermIndex.get());
                    answer = revisedAnswer;

                    collectedDistincts++;

                    if (collectedDistincts > request.query.desiredNumberOfDistincts) {
                        break;
                    }
                }
            }
        }

        boolean resultsExhausted = request.query.timeRange.smallestTimestamp > requestContext.getTimeIndex().getLargestTimestamp();
        DistinctCountAnswer result = new DistinctCountAnswer(aggregateTerms, collectedDistincts, resultsExhausted);
        log.debug("result={}", result);
        return result;
    }

}
