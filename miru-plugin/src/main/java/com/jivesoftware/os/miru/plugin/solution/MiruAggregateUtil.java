package com.jivesoftware.os.miru.plugin.solution;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.bitmap.CardinalityAndLastSetBit;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.bitmap.ReusableBuffers;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndexProvider;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.index.MiruTxIndex;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.mutable.MutableInt;

import static com.google.common.base.Preconditions.checkState;

/**
 *
 */
public class MiruAggregateUtil {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public <BM extends IBM, IBM, S extends MiruSipCursor<S>> void stream(MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<IBM, S> requestContext,
        BM answer,
        Optional<BM> counter,
        int pivotFieldId,
        String streamField,
        CallbackStream<MiruTermCount> terms,
        StackBuffer stackBuffer)
        throws Exception {

        boolean traceEnabled = LOG.isTraceEnabled();
        boolean debugEnabled = LOG.isDebugEnabled();

        if (bitmaps.supportsInPlace()) {
            BM copy = bitmaps.create();
            bitmaps.copy(copy, answer);
            answer = copy;
        }

        final AtomicLong bytesTraversed = new AtomicLong();
        if (debugEnabled) {
            bytesTraversed.addAndGet(bitmaps.sizeInBytes(answer));
        }

        CardinalityAndLastSetBit answerCollector = null;
        ReusableBuffers<BM, IBM> reusable = new ReusableBuffers<>(bitmaps, 2);
        MiruFieldIndex<IBM> fieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);
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

            MiruTermId[] fieldValues = requestContext.getActivityIndex().get(lastSetBit, streamFieldId, stackBuffer);
            if (traceEnabled) {
                LOG.trace("stream: fieldValues={}", new Object[]{fieldValues});
            }
            if (fieldValues == null || fieldValues.length == 0) {
                // could make this a reusable buffer, but this is effectively an error case and would require 3 buffers
                BM removeUnknownField = bitmaps.createWithBits(lastSetBit);
                if (debugEnabled) {
                    bytesTraversed.addAndGet(Math.max(bitmaps.sizeInBytes(answer), bitmaps.sizeInBytes(removeUnknownField)));
                }

                if (bitmaps.supportsInPlace()) {
                    answerCollector = bitmaps.inPlaceAndNotWithCardinalityAndLastSetBit(answer, removeUnknownField);
                } else {
                    BM revisedAnswer = reusable.next();
                    answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, removeUnknownField);
                    answer = revisedAnswer;
                }
            } else {
                //TODO Ideally we should prohibit streaming of multi-term fields because the operation is inherently lossy/ambiguous.
                //TODO Each andNot iteration can potentially mask/remove other distincts which will therefore never be streamed.
                MiruTermId pivotTerm = fieldValues[0];

                MiruInvertedIndex<IBM> invertedIndex = fieldIndex.get(pivotFieldId, pivotTerm);
                Optional<IBM> optionalTermIndex = invertedIndex.getIndex(stackBuffer);
                checkState(optionalTermIndex.isPresent(), "Unable to load inverted index for aggregateTermId: " + pivotTerm);
                IBM termIndex = optionalTermIndex.get();

                if (debugEnabled) {
                    bytesTraversed.addAndGet(Math.max(bitmaps.sizeInBytes(answer), bitmaps.sizeInBytes(termIndex)));
                }

                if (bitmaps.supportsInPlace()) {
                    answerCollector = bitmaps.inPlaceAndNotWithCardinalityAndLastSetBit(answer, termIndex);
                } else {
                    BM revisedAnswer = reusable.next();
                    answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, termIndex);
                    answer = revisedAnswer;
                }

                long afterCount;
                if (counter.isPresent()) {
                    if (bitmaps.supportsInPlace()) {
                        CardinalityAndLastSetBit counterCollector = bitmaps.inPlaceAndNotWithCardinalityAndLastSetBit(counter.get(), termIndex);
                        afterCount = counterCollector.cardinality;
                    } else {
                        BM revisedCounter = reusable.next();
                        CardinalityAndLastSetBit counterCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedCounter, counter.get(), termIndex);
                        counter = Optional.of(revisedCounter);
                        afterCount = counterCollector.cardinality;
                    }
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

    public <BM extends IBM, IBM, S extends MiruSipCursor<S>> void gather(MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<IBM, S> requestContext,
        BM answer,
        int pivotFieldId,
        int batchSize,
        MiruSolutionLog solutionLog,
        Collection<MiruTermId> result,
        StackBuffer stackBuffer) throws Exception {

        MiruActivityIndex activityIndex = requestContext.getActivityIndex();
        MiruFieldIndex<IBM> primaryFieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);

        if (bitmaps.supportsInPlace()) {
            BM copy = bitmaps.create();
            bitmaps.copy(copy, answer);
            answer = copy;
        }

        Set<MiruTermId> distincts = new HashSet<>();
        int[] ids = new int[batchSize];
        int gets = 0;
        int fetched = 0;
        int used = 0;
        long getAllCost = 0;
        long andNotCost = 0;
        while (!bitmaps.isEmpty(answer)) {
            MiruIntIterator intIterator = bitmaps.intIterator(answer);
            int added = 0;
            Arrays.fill(ids, -1);
            while (intIterator.hasNext() && added < batchSize) {
                ids[added] = intIterator.next();
                added++;
            }
            fetched += added;

            int[] actualIds = new int[added];
            System.arraycopy(ids, 0, actualIds, 0, added);
            BM seen = bitmaps.createWithBits(actualIds);

            gets++;
            if (bitmaps.supportsInPlace()) {
                bitmaps.inPlaceAndNot(answer, seen);
                long start = System.nanoTime();
                List<MiruTermId[]> all = activityIndex.getAll(actualIds, pivotFieldId, stackBuffer);
                getAllCost += (System.nanoTime() - start);
                done:
                for (MiruTermId[] termIds : all) {
                    if (termIds != null && termIds.length > 0) {
                        for (MiruTermId termId : termIds) {
                            if (distincts.add(termId)) {
                                result.add(termId);
                                start = System.nanoTime();
                                MiruInvertedIndex<IBM> invertedIndex = primaryFieldIndex.get(pivotFieldId, termId);
                                start = System.nanoTime();
                                bitmaps.inPlaceAndNot(answer, invertedIndex, stackBuffer);
                                andNotCost += (System.nanoTime() - start);
                            }
                        }
                    }
                }
            } else {
                List<IBM> andNots = new ArrayList<>();
                andNots.add(seen);
                List<MiruTermId[]> all = activityIndex.getAll(actualIds, pivotFieldId, stackBuffer);
                for (MiruTermId[] termIds : all) {
                    if (termIds != null && termIds.length > 0) {
                        used++;
                        for (MiruTermId termId : termIds) {
                            if (distincts.add(termId)) {
                                result.add(termId);
                                MiruInvertedIndex<IBM> invertedIndex = primaryFieldIndex.get(pivotFieldId, termId);
                                Optional<IBM> gotIndex = invertedIndex.getIndex(stackBuffer);
                                if (gotIndex.isPresent()) {
                                    andNots.add(gotIndex.get());
                                }
                            }
                        }
                    }
                }

                BM reduced = bitmaps.create();
                bitmaps.andNot(reduced, answer, andNots);
                answer = reduced;
            }
        }
        solutionLog.log(MiruSolutionLogLevel.INFO, "gather aggregate gets:{} fetched:{} used:{} results:{} getAllCost:{} andNotCost:{}",
            gets, fetched, used, result.size(), getAllCost, andNotCost);
    }

    public <BM extends IBM, IBM> BM filter(MiruBitmaps<BM, IBM> bitmaps,
        MiruSchema schema,
        final MiruTermComposer termComposer,
        final MiruFieldIndexProvider<IBM> fieldIndexProvider,
        MiruFilter filter,
        MiruSolutionLog solutionLog,
        Map<FieldAndTermId, MutableInt> termCollector,
        int largestIndex,
        final int considerIfIndexIdGreaterThanN,
        StackBuffer stackBuffer)
        throws Exception {
        return filterInOut(bitmaps, schema, termComposer, fieldIndexProvider, filter, solutionLog, termCollector, true,
            largestIndex, considerIfIndexIdGreaterThanN, stackBuffer);
    }

    private <BM extends IBM, IBM> BM filterInOut(MiruBitmaps<BM, IBM> bitmaps,
        MiruSchema schema,
        final MiruTermComposer termComposer,
        final MiruFieldIndexProvider<IBM> fieldIndexProvider,
        MiruFilter filter,
        MiruSolutionLog solutionLog,
        Map<FieldAndTermId, MutableInt> termCollector,
        boolean termIn,
        int largestIndex,
        final int considerIfIndexIdGreaterThanN,
        StackBuffer stackBuffer)
        throws Exception {

        List<MiruTxIndex<IBM>> filterBitmaps = new ArrayList<>();
        if (filter.inclusiveFilter) {
            filterBitmaps.add(new SimpleInvertedIndex<>(bitmaps.buildIndexMask(largestIndex, Optional.<IBM>absent())));
        }
        if (filter.fieldFilters != null) {
            boolean abortIfEmpty = filter.operation == MiruFilterOperation.and;
            for (final MiruFieldFilter fieldFilter : filter.fieldFilters) {
                final int fieldId = schema.getFieldId(fieldFilter.fieldName);
                final MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(fieldId);
                if (fieldId >= 0) {
                    final List<MiruTxIndex<IBM>> fieldBitmaps = new ArrayList<>();
                    boolean fieldTermIn = filter.operation == MiruFilterOperation.pButNotQ && !filterBitmaps.isEmpty() ? !termIn : termIn;
                    long start = System.currentTimeMillis();
                    List<String> values = fieldFilter.values != null ? fieldFilter.values : Collections.emptyList();
                    for (final String term : values) {
                        if (fieldDefinition.prefix.type != MiruFieldDefinition.Prefix.Type.none && term.endsWith("*")) {
                            String baseTerm = term.substring(0, term.length() - 1);
                            byte[] lowerInclusive = termComposer.prefixLowerInclusive(fieldDefinition.prefix, baseTerm);
                            byte[] upperExclusive = termComposer.prefixUpperExclusive(fieldDefinition.prefix, baseTerm);
                            fieldIndexProvider.getFieldIndex(fieldFilter.fieldType).streamTermIdsForField(fieldId,
                                Collections.singletonList(new KeyRange(lowerInclusive, upperExclusive)),
                                termId -> {
                                    if (termId != null) {
                                        getAndCollectTerm(fieldIndexProvider, fieldFilter, fieldId, termId,
                                            considerIfIndexIdGreaterThanN, fieldTermIn, termCollector, fieldBitmaps);
                                    }
                                    return true;
                                }, stackBuffer);
                        } else {
                            MiruTermId termId = termComposer.compose(fieldDefinition, term);
                            getAndCollectTerm(fieldIndexProvider, fieldFilter, fieldId, termId,
                                considerIfIndexIdGreaterThanN, fieldTermIn, termCollector, fieldBitmaps);
                        }
                    }
                    List<MiruTermId> rawValues = fieldFilter.rawValues != null ? fieldFilter.rawValues : Collections.emptyList();
                    for (MiruTermId termId : rawValues) {
                        getAndCollectTerm(fieldIndexProvider, fieldFilter, fieldId, termId,
                            considerIfIndexIdGreaterThanN, fieldTermIn, termCollector, fieldBitmaps);
                    }
                    solutionLog.log(MiruSolutionLogLevel.DEBUG, "filter: fieldId={} values={} rawValues={} lookup took {} millis.",
                        fieldId, values.size(), rawValues.size(), System.currentTimeMillis() - start);
                    if (abortIfEmpty && fieldBitmaps.isEmpty()) {
                        // implicitly empty results, "and" operation would also be empty
                        return bitmaps.create();
                    } else if (!fieldBitmaps.isEmpty()) {
                        if (fieldBitmaps.size() == 1) {
                            filterBitmaps.add(fieldBitmaps.get(0));
                        } else {
                            start = System.currentTimeMillis();
                            BM r = bitmaps.orTx(fieldBitmaps, stackBuffer);
                            filterBitmaps.add(new SimpleInvertedIndex<>(r));
                            solutionLog.log(MiruSolutionLogLevel.DEBUG, "filter: fieldId={} bitmaps={} aggregate took {} millis.",
                                fieldId, fieldBitmaps.size(), System.currentTimeMillis() - start);
                        }
                    }
                }
            }
        }
        if (filter.subFilters != null) {
            for (MiruFilter subFilter : filter.subFilters) {
                boolean subTermIn = (filter.operation == MiruFilterOperation.pButNotQ && !filterBitmaps.isEmpty()) ? !termIn : termIn;
                BM subStorage = filterInOut(bitmaps, schema, termComposer, fieldIndexProvider, subFilter, solutionLog,
                    termCollector, subTermIn, largestIndex, considerIfIndexIdGreaterThanN, stackBuffer);
                filterBitmaps.add(new SimpleInvertedIndex<>(subStorage));
            }
        }
        return executeFilter(bitmaps, filter.operation, solutionLog, filterBitmaps, stackBuffer);
    }

    private <IBM> void getAndCollectTerm(MiruFieldIndexProvider<IBM> fieldIndexProvider,
        MiruFieldFilter fieldFilter,
        int fieldId,
        MiruTermId termId,
        int considerIfIndexIdGreaterThanN,
        boolean fieldTermIn,
        Map<FieldAndTermId, MutableInt> termCollector,
        List<MiruTxIndex<IBM>> fieldBitmaps) throws Exception {

        MiruInvertedIndex<IBM> got = fieldIndexProvider.getFieldIndex(fieldFilter.fieldType).get(
            fieldId,
            termId,
            considerIfIndexIdGreaterThanN);
        fieldBitmaps.add(got);
        collectTerm(fieldId, termId, fieldTermIn, termCollector);
    }

    private void collectTerm(int fieldId, MiruTermId termId, boolean termIn, Map<FieldAndTermId, MutableInt> termCollector) {
        if (termCollector != null) {
            MutableInt count = termCollector.computeIfAbsent(new FieldAndTermId(fieldId, termId), key -> new MutableInt());
            if (termIn) {
                count.increment();
            } else {
                count.decrement();
            }
        }
    }

    private <BM extends IBM, IBM> BM executeFilter(MiruBitmaps<BM, IBM> bitmaps,
        MiruFilterOperation operation,
        MiruSolutionLog solutionLog,
        List<MiruTxIndex<IBM>> filterBitmaps,
        StackBuffer stackBuffer) throws Exception {

        BM bitmapStorage;
        long start = System.currentTimeMillis();
        if (operation == MiruFilterOperation.and) {
            bitmapStorage = bitmaps.andTx(filterBitmaps, stackBuffer);
        } else if (operation == MiruFilterOperation.or) {
            bitmapStorage = bitmaps.orTx(filterBitmaps, stackBuffer);
        } else if (operation == MiruFilterOperation.pButNotQ) {
            bitmapStorage = bitmaps.andNotTx(filterBitmaps.get(0), filterBitmaps.subList(1, filterBitmaps.size()), stackBuffer);
        } else {
            throw new UnsupportedOperationException(operation + " isn't currently supported.");
        }
        solutionLog.log(MiruSolutionLogLevel.DEBUG, "executeFilter: aggregate took {} millis.", System.currentTimeMillis() - start);
        return bitmapStorage;
    }

    @Override
    public String toString() {
        return "MiruAggregateUtil{" + '}';
    }

    private static class SimpleInvertedIndex<IBM> implements MiruTxIndex<IBM> {
        private final IBM bitmap;

        public SimpleInvertedIndex(IBM bitmap) {
            this.bitmap = bitmap;
        }

        @Override
        public <R> R txIndex(IndexTx<R, IBM> tx, StackBuffer stackBuffer) throws Exception {
            return tx.tx(bitmap, null, -1, null);
        }
    }

}
