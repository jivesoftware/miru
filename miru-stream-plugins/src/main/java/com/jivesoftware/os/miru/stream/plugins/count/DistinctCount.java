package com.jivesoftware.os.miru.stream.plugins.count;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.bitmap.CardinalityAndLastSetBit;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
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

    public <BM extends IBM, IBM> DistinctCountAnswer numberOfDistincts(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> requestContext,
        MiruRequest<DistinctCountQuery> request,
        Optional<DistinctCountReport> lastReport,
        BM answer)
        throws Exception {

        StackBuffer stackBuffer = new StackBuffer();

        log.debug("Get number of distincts for answer={} query={}", answer, request);

        int collectedDistincts = 0;
        Set<MiruValue> aggregateTerms;
        if (lastReport.isPresent()) {
            collectedDistincts = lastReport.get().collectedDistincts;
            aggregateTerms = Sets.newHashSet(lastReport.get().aggregateTerms);
        } else {
            aggregateTerms = Sets.newHashSet();
        }

        MiruTermComposer termComposer = requestContext.getTermComposer();
        MiruSchema schema = requestContext.getSchema();

        int fieldId = schema.getFieldId(request.query.aggregateCountAroundField);
        MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(fieldId);
        log.debug("fieldId={}", fieldId);
        if (fieldId >= 0) {
            MiruFieldIndex<BM, IBM> fieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);

            BitmapAndLastId<BM> container = new BitmapAndLastId<>();
            for (MiruValue aggregateTerm : aggregateTerms) {
                MiruTermId aggregateTermId = termComposer.compose(schema, fieldDefinition, stackBuffer, aggregateTerm.parts);
                container.clear();
                fieldIndex.get(name, fieldId, aggregateTermId).getIndex(container, stackBuffer);
                if (!container.isSet()) {
                    continue;
                }

                IBM termIndex = container.getBitmap();
                answer = bitmaps.andNot(answer, termIndex);
            }

            CardinalityAndLastSetBit<BM> answerCollector = null;
            while (true) {
                int lastSetBit = answerCollector == null ? bitmaps.lastSetBit(answer) : answerCollector.lastSetBit;
                log.trace("lastSetBit={}", lastSetBit);
                if (lastSetBit < 0) {
                    break;
                }
                MiruTermId[] fieldValues = requestContext.getActivityIndex().get(name, lastSetBit, fieldDefinition, stackBuffer);
                log.trace("fieldValues={}", (Object) fieldValues);
                if (fieldValues == null || fieldValues.length == 0) {
                    // could make this a reusable buffer, but this is effectively an error case and would require 3 buffers
                    BM removeUnknownField = bitmaps.createWithBits(lastSetBit);
                    answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(answer, removeUnknownField);
                    answer = answerCollector.bitmap;

                } else {
                    MiruTermId aggregateTermId = fieldValues[0];
                    MiruValue aggregateTerm = new MiruValue(termComposer.decompose(schema, fieldDefinition, stackBuffer, aggregateTermId));

                    aggregateTerms.add(aggregateTerm);
                    container.clear();
                    fieldIndex.get(name, fieldId, aggregateTermId).getIndex(container, stackBuffer);
                    checkState(container.isSet(), "Unable to load inverted index for aggregateTermId: %s", aggregateTermId);

                    answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(answer, container.getBitmap());
                    answer = answerCollector.bitmap;

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
