package com.jivesoftware.os.miru.plugin.solution;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.FieldMultiTermTxIndex;
import com.jivesoftware.os.miru.plugin.index.IndexTx;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndexProvider;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.index.MiruTxIndex;
import com.jivesoftware.os.miru.plugin.index.TermIdStream;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 */
public class MiruAggregateUtil {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public <BM extends IBM, IBM, S extends MiruSipCursor<S>> void stream(MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        MiruRequestContext<BM, IBM, S> requestContext,
        MiruPartitionCoord coord,
        BM initialAnswer,
        Optional<BM> optionalCounter,
        int pivotFieldId,
        int batchSize,
        String streamField,
        StackBuffer stackBuffer,
        CallbackStream<MiruTermCount> terms)
        throws Exception {

        boolean traceEnabled = LOG.isTraceEnabled();
        boolean debugEnabled = LOG.isDebugEnabled();

        if (bitmaps.supportsInPlace()) {
            // don't mutate the original
            initialAnswer = bitmaps.copy(initialAnswer);
        }

        final AtomicLong bytesTraversed = new AtomicLong();
        if (debugEnabled) {
            bytesTraversed.addAndGet(bitmaps.sizeInBytes(initialAnswer));
        }

        BM[] answer = bitmaps.createArrayOf(1);
        answer[0] = initialAnswer;

        BM[] counter;
        if (optionalCounter.isPresent()) {
            counter = bitmaps.createArrayOf(1);
            counter[0] = optionalCounter.orNull();
        } else {
            counter = null;
        }

        MiruActivityIndex activityIndex = requestContext.getActivityIndex();
        MiruFieldIndex<BM, IBM> primaryFieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);
        int streamFieldId = requestContext.getSchema().getFieldId(streamField);

        MutableLong beforeCount = new MutableLong(counter != null ? bitmaps.cardinality(counter[0]) : bitmaps.cardinality(answer[0]));
        LOG.debug("stream: streamField={} streamFieldId={} pivotFieldId={} beforeCount={}", streamField, streamFieldId, pivotFieldId, beforeCount);

        FieldMultiTermTxIndex<BM, IBM> multiTermTxIndex = new FieldMultiTermTxIndex<>(primaryFieldIndex, pivotFieldId);
        Map<MiruTermId, MiruTermId[]> distincts = Maps.newHashMapWithExpectedSize(batchSize);

        int[] ids = new int[batchSize];
        while (!bitmaps.isEmpty(answer[0])) {
            MiruIntIterator intIterator = bitmaps.intIterator(answer[0]);
            int added = 0;
            Arrays.fill(ids, -1);
            while (intIterator.hasNext() && added < batchSize) {
                ids[added] = intIterator.next();
                added++;
            }

            int[] actualIds = new int[added];
            System.arraycopy(ids, 0, actualIds, 0, added);

            if (bitmaps.supportsInPlace()) {
                bitmaps.inPlaceRemoveRange(answer[0], actualIds[0], actualIds[actualIds.length - 1] + 1);
            } else {
                answer[0] = bitmaps.removeRange(answer[0], actualIds[0], actualIds[actualIds.length - 1] + 1);
            }

            List<MiruTermId[]> all = activityIndex.getAll(actualIds, pivotFieldId, stackBuffer);
            List<MiruTermId[]> streamAll;
            if (streamFieldId != pivotFieldId) {
                streamAll = activityIndex.getAll(actualIds, streamFieldId, stackBuffer);
            } else {
                streamAll = all;
            }
            distincts.clear();
            for (int i = 0; i < all.size(); i++) {
                MiruTermId[] termIds = all.get(i);
                MiruTermId[] streamTermIds = streamAll.get(i);
                if (termIds != null && termIds.length > 0) {
                    for (int j = 0; j < termIds.length; j++) {
                        distincts.put(termIds[j], streamTermIds);
                    }
                }
            }

            MiruTermId[] termIds = distincts.keySet().toArray(new MiruTermId[distincts.size()]);
            long[] termCounts = new long[termIds.length];
            multiTermTxIndex.setTermIds(termIds);

            bitmaps.multiTx(
                (tx, stackBuffer1) -> primaryFieldIndex.multiTxIndex(pivotFieldId, termIds, stackBuffer1, tx),
                (index, bitmap) -> {
                    if (bitmaps.supportsInPlace()) {
                        bitmaps.inPlaceAndNot(answer[0], bitmap);
                        if (counter != null) {
                            bitmaps.inPlaceAndNot(counter[0], bitmap);
                        }
                    } else {
                        answer[0] = bitmaps.andNot(answer[0], bitmap);
                        if (counter != null) {
                            counter[0] = bitmaps.andNot(counter[0], bitmap);
                        }
                    }
                    long afterCount = (counter != null) ? bitmaps.cardinality(counter[0]) : bitmaps.cardinality(answer[0]);
                    termCounts[index] = beforeCount.longValue() - afterCount;
                    beforeCount.setValue(afterCount);
                },
                stackBuffer);

            for (int i = 0; i < termIds.length; i++) {
                MiruTermCount termCount = new MiruTermCount(termIds[i], distincts.get(termIds[i]), termCounts[i]);
                if (terms.callback(termCount) != termCount) {
                    return;
                }
            }
        }
        terms.callback(null); // EOS
        LOG.debug("stream: bytesTraversed={}", bytesTraversed.longValue());
    }

    public <BM extends IBM, IBM, S extends MiruSipCursor<S>> void gather(MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, S> requestContext,
        BM answer,
        int pivotFieldId,
        int batchSize,
        MiruSolutionLog solutionLog,
        TermIdStream termIdStream,
        StackBuffer stackBuffer) throws Exception {

        MiruActivityIndex activityIndex = requestContext.getActivityIndex();
        MiruFieldIndex<BM, IBM> primaryFieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);

        if (bitmaps.supportsInPlace()) {
            // don't mutate the original
            answer = bitmaps.copy(answer);
        }

        FieldMultiTermTxIndex<BM, IBM> multiTermTxIndex = new FieldMultiTermTxIndex<>(primaryFieldIndex, pivotFieldId);
        Set<MiruTermId> distincts = Sets.newHashSetWithExpectedSize(batchSize);

        int[] ids = new int[batchSize];
        int gets = 0;
        int fetched = 0;
        int used = 0;
        long getAllCost = 0;
        long andNotCost = 0;
        done:
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

            gets++;

            if (bitmaps.supportsInPlace()) {
                bitmaps.inPlaceRemoveRange(answer, actualIds[0], actualIds[actualIds.length - 1] + 1);
            } else {
                answer = bitmaps.removeRange(answer, actualIds[0], actualIds[actualIds.length - 1] + 1);
            }

            long start = System.nanoTime();
            List<MiruTermId[]> all = activityIndex.getAll(actualIds, pivotFieldId, stackBuffer);
            getAllCost += (System.nanoTime() - start);
            distincts.clear();
            for (MiruTermId[] termIds : all) {
                if (termIds != null && termIds.length > 0) {
                    for (MiruTermId termId : termIds) {
                        if (distincts.add(termId)) {
                            if (!termIdStream.stream(termId)) {
                                break done;
                            }
                        }
                    }
                }
            }

            MiruTermId[] termIds = distincts.toArray(new MiruTermId[distincts.size()]);
            multiTermTxIndex.setTermIds(termIds);

            start = System.nanoTime();
            if (bitmaps.supportsInPlace()) {
                bitmaps.inPlaceAndNotMultiTx(answer, multiTermTxIndex, stackBuffer);
            } else {
                answer = bitmaps.andNotMultiTx(answer, multiTermTxIndex, stackBuffer);
            }
            andNotCost += (System.nanoTime() - start);
        }
        solutionLog.log(MiruSolutionLogLevel.INFO, "gather aggregate gets:{} fetched:{} used:{} getAllCost:{} andNotCost:{}",
            gets, fetched, used, getAllCost, andNotCost);
    }

    public <BM extends IBM, IBM> BM filter(MiruBitmaps<BM, IBM> bitmaps,
        MiruSchema schema,
        final MiruTermComposer termComposer,
        final MiruFieldIndexProvider<BM, IBM> fieldIndexProvider,
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
        final MiruFieldIndexProvider<BM, IBM> fieldIndexProvider,
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
                    List<MiruValue> values = fieldFilter.values != null ? fieldFilter.values : Collections.emptyList();
                    for (final MiruValue value : values) {
                        if (fieldDefinition.prefix.type != MiruFieldDefinition.Prefix.Type.none && value.last().equals("*")) {
                            String[] baseParts = value.slice(0, value.parts.length - 1);
                            byte[] lowerInclusive = termComposer.prefixLowerInclusive(schema, fieldDefinition, stackBuffer, baseParts);
                            byte[] upperExclusive = termComposer.prefixUpperExclusive(schema, fieldDefinition, stackBuffer, baseParts);
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
                            MiruTermId termId = termComposer.compose(schema, fieldDefinition, stackBuffer, value.parts);
                            getAndCollectTerm(fieldIndexProvider, fieldFilter, fieldId, termId,
                                considerIfIndexIdGreaterThanN, fieldTermIn, termCollector, fieldBitmaps);
                        }
                    }
                    solutionLog.log(MiruSolutionLogLevel.DEBUG, "filter: fieldId={} values={} lookup took {} millis.",
                        fieldId, values.size(), System.currentTimeMillis() - start);
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

    private <BM extends IBM, IBM> void getAndCollectTerm(MiruFieldIndexProvider<BM, IBM> fieldIndexProvider,
        MiruFieldFilter fieldFilter,
        int fieldId,
        MiruTermId termId,
        int considerIfIndexIdGreaterThanN,
        boolean fieldTermIn,
        Map<FieldAndTermId, MutableInt> termCollector,
        List<MiruTxIndex<IBM>> fieldBitmaps) throws Exception {

        MiruInvertedIndex<BM, IBM> got = fieldIndexProvider.getFieldIndex(fieldFilter.fieldType).get(
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
