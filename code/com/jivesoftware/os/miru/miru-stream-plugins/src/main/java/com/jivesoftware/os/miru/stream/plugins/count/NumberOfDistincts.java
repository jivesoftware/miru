package com.jivesoftware.os.miru.stream.plugins.count;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
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
import java.util.Collections;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

/**
 *
 */
public class NumberOfDistincts {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    public <BM> DistinctCountAnswer numberOfDistincts(MiruBitmaps<BM> bitmaps,
        MiruRequestContext<BM> requestContext,
        MiruRequest<DistinctCountQuery> request,
        Optional<DistinctCountReport> lastReport,
        BM answer)
        throws Exception {

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
                Optional<BM> optionalTermIndex = fieldIndex.get(fieldId, aggregateTermId).getIndex();
                if (!optionalTermIndex.isPresent()) {
                    continue;
                }

                BM termIndex = optionalTermIndex.get();

                BM revisedAnswer = reusable.next();
                bitmaps.andNot(revisedAnswer, answer, Collections.singletonList(termIndex));
                answer = revisedAnswer;
            }

            CardinalityAndLastSetBit answerCollector = null;
            while (true) {
                int lastSetBit = answerCollector == null ? bitmaps.lastSetBit(answer) : answerCollector.lastSetBit;
                log.trace("lastSetBit={}", lastSetBit);
                if (lastSetBit < 0) {
                    break;
                }
                MiruTermId[] fieldValues = requestContext.getActivityIndex().get(request.tenantId, lastSetBit, fieldId);
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
                    Optional<BM> optionalTermIndex = fieldIndex.get(fieldId, aggregateTermId).getIndex();
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
        DistinctCountAnswer result = new DistinctCountAnswer(ImmutableSet.copyOf(aggregateTerms), collectedDistincts);
        log.debug("result={}", result);
        return result;
    }

}
