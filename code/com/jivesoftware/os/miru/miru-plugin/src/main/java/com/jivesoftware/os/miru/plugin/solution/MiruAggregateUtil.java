package com.jivesoftware.os.miru.plugin.solution;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.Charsets;

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
        int fieldId = requestContext.getSchema().getFieldId(streamField);
        long beforeCount = counter.isPresent() ? bitmaps.cardinality(counter.get()) : bitmaps.cardinality(answer);
        LOG.debug("stream: field={} fieldId={} beforeCount={}", streamField, fieldId, beforeCount);
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

            MiruTermId[] fieldValues = requestContext.getActivityIndex().get(tenantId, lastSetBit, fieldId);
            if (traceEnabled) {
                LOG.trace("stream: fieldValues={}", new Object[] { fieldValues });
            }
            if (fieldValues == null || fieldValues.length == 0) {
                // could make this a reusable buffer, but this is effectively an error case and would require 3 buffers
                BM removeUnknownField = bitmaps.create();
                bitmaps.set(removeUnknownField, lastSetBit);
                if (debugEnabled) {
                    bytesTraversed.addAndGet(Math.max(bitmaps.sizeInBytes(answer), bitmaps.sizeInBytes(removeUnknownField)));
                }

                BM revisedAnswer = reusable.next();
                answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, removeUnknownField);
                answer = revisedAnswer;

            } else {
                MiruTermId pivotTerm = fieldValues[0]; // Kinda lame but for now we don't see a need for multi field aggregation.
                //And by "for now" we mean IT DOESN'T FUCKING WORK.
                //Because each andNot iteration can potentially mask/remove other distincts further along in the answer.
                //Instead, you need to use write-time aggregation to record the latest occurrence of each distinct value, and traverse the aggregate bitmap.

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
        MiruFieldIndexProvider<BM> fieldIndexProvider,
        MiruFilter filter,
        MiruSolutionLog solutionLog,
        BM bitmapStorage,
        int considerIfIndexIdGreaterThanN)
        throws Exception {

        List<BM> filterBitmaps = new ArrayList<>();
        if (filter.fieldFilters.isPresent()) {
            for (MiruFieldFilter fieldFilter : filter.fieldFilters.get()) {
                int fieldId = schema.getFieldId(fieldFilter.fieldName);
                if (fieldId >= 0) {
                    List<BM> fieldBitmaps = new ArrayList<>();
                    long start = System.currentTimeMillis();
                    for (String term : fieldFilter.values) {
                        MiruInvertedIndex<BM> got = fieldIndexProvider.getFieldIndex(fieldFilter.fieldType).get(
                            fieldId,
                            new MiruTermId(term.getBytes(Charsets.UTF_8)),
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
        if (filter.subFilter.isPresent()) {
            for (MiruFilter subFilter : filter.subFilter.get()) {
                BM subStorage = bitmaps.create();
                filter(bitmaps, schema, fieldIndexProvider, subFilter, solutionLog, subStorage, considerIfIndexIdGreaterThanN);
                filterBitmaps.add(subStorage);
            }
        }
        executeFilter(bitmaps, filter, solutionLog, bitmapStorage, filterBitmaps);
    }

    private <BM> void executeFilter(MiruBitmaps<BM> bitmaps,
        MiruFilter filter,
        MiruSolutionLog solutionLog,
        BM bitmapStorage,
        List<BM> filterBitmaps) {

        long start = System.currentTimeMillis();
        if (filter.operation == MiruFilterOperation.and) {
            bitmaps.and(bitmapStorage, filterBitmaps);
        } else if (filter.operation == MiruFilterOperation.or) {
            bitmaps.or(bitmapStorage, filterBitmaps);
        } else if (filter.operation == MiruFilterOperation.pButNotQ) {
            bitmaps.andNot(bitmapStorage, filterBitmaps.get(0), filterBitmaps.subList(1, filterBitmaps.size()));
        } else {
            throw new UnsupportedOperationException(filter.operation + " isn't currently supported.");
        }
        solutionLog.log(MiruSolutionLogLevel.DEBUG, "executeFilter: aggregate took {} millis.", System.currentTimeMillis() - start);
    }

}
