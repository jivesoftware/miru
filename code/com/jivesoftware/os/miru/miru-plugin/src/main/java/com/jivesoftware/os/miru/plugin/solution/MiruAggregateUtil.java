package com.jivesoftware.os.miru.plugin.solution;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.plugin.bitmap.CardinalityAndLastSetBit;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.ReusableBuffers;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndexProvider;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;

/**
 *
 */
public class MiruAggregateUtil {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public <BM> void stream(MiruBitmaps<BM> bitmaps,
        MiruTenantId tenantId,
        MiruRequestContext<BM> requestContext,
        BM answer,
        Optional<BM> counter,
        int pivotFieldId,
        String streamField,
        CallbackStream<MiruTermCount> terms)
        throws Exception {

        boolean traceEnabled = LOG.isTraceEnabled();
        boolean debugEnabled = LOG.isDebugEnabled();

        final AtomicLong bytesTraversed = new AtomicLong();
        if (debugEnabled) {
            bytesTraversed.addAndGet(bitmaps.sizeInBytes(answer));
        }
        CardinalityAndLastSetBit answerCollector = null;
        ReusableBuffers<BM> reusable = new ReusableBuffers<>(bitmaps, 2);
        MiruFieldIndex<BM> fieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);
        int streamFieldId = requestContext.getSchema().getFieldId(streamField);
        long beforeCount = counter.isPresent() ? bitmaps.cardinality(counter.get()) : bitmaps.cardinality(answer);
        LOG.debug("stream: streamField={} streamFieldId={} pivotFieldId={} beforeCount={}", streamField, streamFieldId, pivotFieldId, beforeCount);
        int priorLastSetBit = Integer.MAX_VALUE;
        while (true) {
            int lastSetBit = answerCollector == null ? bitmaps.lastSetBit(answer) : answerCollector.lastSetBit;
            LOG.trace("stream: lastSetBit={}", lastSetBit);
            if (priorLastSetBit <= lastSetBit) {
                LOG.error("Failed to make forward progress removing lastSetBit:{} answer:{}", lastSetBit, answer);
                break;
            }
            priorLastSetBit = lastSetBit;
            if (lastSetBit < 0) {
                break;
            }

            MiruTermId[] fieldValues = requestContext.getActivityIndex().get(tenantId, lastSetBit, streamFieldId);
            if (traceEnabled) {
                LOG.trace("stream: fieldValues={}", new Object[] { fieldValues });
            }
            if (fieldValues == null || fieldValues.length == 0) {
                // could make this a reusable buffer, but this is effectively an error case and would require 3 buffers
                BM removeUnknownField = bitmaps.createWithBits(lastSetBit);
                if (debugEnabled) {
                    bytesTraversed.addAndGet(Math.max(bitmaps.sizeInBytes(answer), bitmaps.sizeInBytes(removeUnknownField)));
                }

                BM revisedAnswer = reusable.next();
                answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, removeUnknownField);
                answer = revisedAnswer;

            } else {
                //TODO Ideally we should prohibit streaming of multi-term fields because the operation is inherently lossy/ambiguous.
                //TODO Each andNot iteration can potentially mask/remove other distincts which will therefore never be streamed.
                MiruTermId pivotTerm = fieldValues[0];

                MiruInvertedIndex<BM> invertedIndex = fieldIndex.get(pivotFieldId, pivotTerm);
                Optional<BM> optionalTermIndex = invertedIndex.getIndex();
                checkState(optionalTermIndex.isPresent(), "Unable to load inverted index for aggregateTermId: " + pivotTerm);
                BM termIndex = optionalTermIndex.get();

                if (debugEnabled) {
                    bytesTraversed.addAndGet(Math.max(bitmaps.sizeInBytes(answer), bitmaps.sizeInBytes(termIndex)));
                }

                BM revisedAnswer = reusable.next();
                answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, termIndex);
                answer = revisedAnswer;

                long afterCount;
                if (counter.isPresent()) {
                    BM revisedCounter = reusable.next();
                    CardinalityAndLastSetBit counterCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedCounter, counter.get(), termIndex);
                    counter = Optional.of(revisedCounter);
                    afterCount = counterCollector.cardinality;
                } else {
                    afterCount = answerCollector.cardinality;
                }

                MiruTermCount termCount = new MiruTermCount(pivotTerm, fieldValues, beforeCount - afterCount);
                if (termCount != terms.callback(termCount)) { // Stop stream
                    return;
                }
                beforeCount = afterCount;
            }

        }
        terms.callback(null); // EOS
        LOG.debug("stream: bytesTraversed={}", bytesTraversed.longValue());
    }

    public <BM> void filter(MiruBitmaps<BM> bitmaps,
        MiruSchema schema,
        MiruTermComposer termComposer,
        MiruFieldIndexProvider<BM> fieldIndexProvider,
        MiruFilter filter,
        MiruSolutionLog solutionLog,
        BM bitmapStorage,
        int largestIndex,
        int considerIfIndexIdGreaterThanN)
        throws Exception {

        List<BM> filterBitmaps = new ArrayList<>();
        if (filter.inclusiveFilter) {
            filterBitmaps.add(bitmaps.buildIndexMask(largestIndex, Optional.<BM>absent()));
        }
        if (filter.fieldFilters != null) {
            for (MiruFieldFilter fieldFilter : filter.fieldFilters) {
                int fieldId = schema.getFieldId(fieldFilter.fieldName);
                MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(fieldId);
                if (fieldId >= 0) {
                    List<BM> fieldBitmaps = new ArrayList<>();
                    long start = System.currentTimeMillis();
                    for (String term : fieldFilter.values) {
                        MiruInvertedIndex<BM> got = fieldIndexProvider.getFieldIndex(fieldFilter.fieldType).get(
                            fieldId,
                            termComposer.compose(fieldDefinition, term),
                            considerIfIndexIdGreaterThanN);
                        Optional<BM> index = got.getIndex();
                        if (index.isPresent()) {
                            fieldBitmaps.add(index.get());
                        }
                    }
                    solutionLog.log(MiruSolutionLogLevel.DEBUG, "filter: fieldId={} values={} lookup took {} millis.",
                        fieldId, fieldFilter.values.size(), System.currentTimeMillis() - start);
                    if (fieldBitmaps.isEmpty() && filter.operation == MiruFilterOperation.and) {
                        // implicitly empty results, "and" operation would also be empty
                        return;
                    } else if (!fieldBitmaps.isEmpty()) {
                        start = System.currentTimeMillis();
                        BM r = bitmaps.create();
                        bitmaps.or(r, fieldBitmaps);
                        filterBitmaps.add(r);
                        solutionLog.log(MiruSolutionLogLevel.DEBUG, "filter: fieldId={} bitmaps={} aggregate took {} millis.",
                            fieldId, fieldBitmaps.size(), System.currentTimeMillis() - start);
                    }
                }
            }
        }
        if (filter.subFilters != null) {
            for (MiruFilter subFilter : filter.subFilters) {
                BM subStorage = bitmaps.create();
                filter(bitmaps, schema, termComposer, fieldIndexProvider, subFilter, solutionLog, subStorage, largestIndex, considerIfIndexIdGreaterThanN);
                filterBitmaps.add(subStorage);
            }
        }
        executeFilter(bitmaps, filter.operation, solutionLog, bitmapStorage, filterBitmaps);
    }

    private <BM> void executeFilter(MiruBitmaps<BM> bitmaps,
        MiruFilterOperation operation,
        MiruSolutionLog solutionLog,
        BM bitmapStorage,
        List<BM> filterBitmaps) {

        long start = System.currentTimeMillis();
        if (operation == MiruFilterOperation.and) {
            bitmaps.and(bitmapStorage, filterBitmaps);
        } else if (operation == MiruFilterOperation.or) {
            bitmaps.or(bitmapStorage, filterBitmaps);
        } else if (operation == MiruFilterOperation.pButNotQ) {
            bitmaps.andNot(bitmapStorage, filterBitmaps.get(0), filterBitmaps.subList(1, filterBitmaps.size()));
        } else {
            throw new UnsupportedOperationException(operation + " isn't currently supported.");
        }
        solutionLog.log(MiruSolutionLogLevel.DEBUG, "executeFilter: aggregate took {} millis.", System.currentTimeMillis() - start);
    }

}
