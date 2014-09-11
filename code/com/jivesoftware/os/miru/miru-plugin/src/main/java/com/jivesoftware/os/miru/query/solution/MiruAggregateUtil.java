package com.jivesoftware.os.miru.query.solution;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.query.bitmap.CardinalityAndLastSetBit;
import com.jivesoftware.os.miru.query.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.query.bitmap.ReusableBuffers;
import com.jivesoftware.os.miru.query.context.MiruRequestContext;
import com.jivesoftware.os.miru.query.index.MiruField;
import com.jivesoftware.os.miru.query.index.MiruFields;
import com.jivesoftware.os.miru.query.index.MiruInvertedIndex;
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
            MiruRequestContext stream,
            BM answer,
            Optional<BM> counter,
            MiruField<BM> pivotField,
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
        int fieldId = stream.schema.getFieldId(streamField);
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

            MiruTermId[] fieldValues = stream.activityIndex.get(tenantId, lastSetBit, fieldId);
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

                Optional<MiruInvertedIndex<BM>> invertedIndex = pivotField.getInvertedIndex(pivotTerm);
                checkState(invertedIndex.isPresent(), "Unable to load inverted index for aggregateTermId: " + pivotTerm);

                BM termIndex = invertedIndex.get().getIndex();
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
            MiruFields<BM> fieldIndex,
            MiruFilter filter,
            BM bitmapStorage,
            int considerIfIndexIdGreaterThanN)
            throws Exception {
        List<BM> filterBitmaps = new ArrayList<>();
        if (filter.fieldFilters.isPresent()) {
            for (MiruFieldFilter fieldFilter : filter.fieldFilters.get()) {
                int fieldId = schema.getFieldId(fieldFilter.fieldName);
                if (fieldId >= 0) {
                    MiruField<BM> field = fieldIndex.getField(fieldId);
                    List<BM> fieldBitmaps = new ArrayList<>();
                    for (String term : fieldFilter.values) {
                        Optional<MiruInvertedIndex<BM>> got = field.getInvertedIndex(
                                new MiruTermId(term.getBytes(Charsets.UTF_8)),
                                considerIfIndexIdGreaterThanN);
                        if (got.isPresent()) {
                            fieldBitmaps.add(got.get().getIndex());
                        }
                    }
                    if (fieldBitmaps.isEmpty() && filter.operation == MiruFilterOperation.and) {
                        // implicitly empty results, "and" operation would also be empty
                        return;
                    } else if (!fieldBitmaps.isEmpty()) {
                        BM r = bitmaps.create();
                        bitmaps.or(r, fieldBitmaps);
                        filterBitmaps.add(r);
                    }
                }
            }
        }
        if (filter.subFilter.isPresent()) {
            for (MiruFilter subFilter : filter.subFilter.get()) {
                BM subStorage = bitmaps.create();
                filter(bitmaps, schema, fieldIndex, subFilter, subStorage, considerIfIndexIdGreaterThanN);
                filterBitmaps.add(subStorage);
            }
        }
        executeFilter(bitmaps, filter, bitmapStorage, filterBitmaps);
    }

    private <BM> void executeFilter(MiruBitmaps<BM> bitmaps,
            MiruFilter filter,
            BM bitmapStorage,
            List<BM> filterBitmaps) {
        if (filter.operation == MiruFilterOperation.and) {
            bitmaps.and(bitmapStorage, filterBitmaps);
        } else if (filter.operation == MiruFilterOperation.or) {
            bitmaps.or(bitmapStorage, filterBitmaps);
        } else if (filter.operation == MiruFilterOperation.pButNotQ) {
            bitmaps.andNot(bitmapStorage, filterBitmaps.get(0), filterBitmaps.subList(1, filterBitmaps.size()));
        } else {
            throw new UnsupportedOperationException(filter.operation + " isn't currently supported.");
        }
    }

}
