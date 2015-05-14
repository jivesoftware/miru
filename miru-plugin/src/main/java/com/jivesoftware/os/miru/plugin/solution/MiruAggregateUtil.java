package com.jivesoftware.os.miru.plugin.solution;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.api.KeyRange;
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
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;

/**
 *
 */
public class MiruAggregateUtil {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public <BM, S extends MiruSipCursor<S>> void stream(MiruBitmaps<BM> bitmaps,
        MiruRequestContext<BM, S> requestContext,
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

            MiruTermId[] fieldValues = requestContext.getActivityIndex().get(lastSetBit, streamFieldId);
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

    public <BM, S extends MiruSipCursor<S>> void gather(MiruBitmaps<BM> bitmaps,
        MiruRequestContext<BM, S> requestContext,
        BM answer,
        int pivotFieldId,
        int batchSize,
        Collection<MiruTermId> result) throws Exception {

        MiruActivityIndex activityIndex = requestContext.getActivityIndex();
        MiruFieldIndex<BM> primaryFieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);

        Set<MiruTermId> distincts = new HashSet<>();
        int[] ids = new int[batchSize];
        while (!bitmaps.isEmpty(answer)) {
            MiruIntIterator intIterator = bitmaps.intIterator(answer);
            int added = 0;
            Arrays.fill(ids, -1);
            while (intIterator.hasNext() && added < batchSize) {
                ids[added] = intIterator.next();
                added++;
            }

            int[] actualIds = new int[added];
            System.arraycopy(ids, 0, actualIds, 0, added);
            BM seen = bitmaps.createWithBits(actualIds);

            List<BM> andNots = new ArrayList<>();
            andNots.add(seen);
            List<MiruTermId[]> all = activityIndex.getAll(actualIds, pivotFieldId);
            for (MiruTermId[] termIds : all) {
                if (termIds != null && termIds.length > 0) {
                    for (MiruTermId termId : termIds) {
                        if (distincts.add(termId)) {
                            result.add(termId);
                            MiruInvertedIndex<BM> invertedIndex = primaryFieldIndex.get(pivotFieldId, termId);
                            Optional<BM> gotIndex = invertedIndex.getIndex();
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

    public <BM> void filter(MiruBitmaps<BM> bitmaps,
        MiruSchema schema,
        final MiruTermComposer termComposer,
        final MiruFieldIndexProvider<BM> fieldIndexProvider,
        MiruFilter filter,
        MiruSolutionLog solutionLog,
        BM bitmapStorage,
        int largestIndex,
        final int considerIfIndexIdGreaterThanN)
        throws Exception {

        List<BM> filterBitmaps = new ArrayList<>();
        if (filter.inclusiveFilter) {
            filterBitmaps.add(bitmaps.buildIndexMask(largestIndex, Optional.<BM>absent()));
        }
        if (filter.fieldFilters != null) {
            for (final MiruFieldFilter fieldFilter : filter.fieldFilters) {
                final int fieldId = schema.getFieldId(fieldFilter.fieldName);
                final MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(fieldId);
                if (fieldId >= 0) {
                    final List<BM> fieldBitmaps = new ArrayList<>();
                    long start = System.currentTimeMillis();
                    for (final String term : fieldFilter.values) {
                        if (fieldDefinition.prefix.type != MiruFieldDefinition.Prefix.Type.none && term.endsWith("*")) {
                            String baseTerm = term.substring(0, term.length() - 1);
                            byte[] lowerInclusive = termComposer.prefixLowerInclusive(fieldDefinition.prefix, baseTerm);
                            byte[] upperExclusive = termComposer.prefixUpperExclusive(fieldDefinition.prefix, baseTerm);
                            fieldIndexProvider.getFieldIndex(fieldFilter.fieldType).streamTermIdsForField(fieldId,
                                Arrays.asList(new KeyRange(lowerInclusive, upperExclusive)),
                                termId -> {
                                    if (termId != null) {
                                        try {
                                            MiruInvertedIndex<BM> got = fieldIndexProvider.getFieldIndex(fieldFilter.fieldType).get(
                                                fieldId,
                                                termId,
                                                considerIfIndexIdGreaterThanN);
                                            Optional<BM> index = got.getIndex();
                                            if (index.isPresent()) {
                                                fieldBitmaps.add(index.get());
                                            }
                                        } catch (Exception e) {
                                            throw new RuntimeException("Failed to get wildcard index", e);
                                        }
                                    }
                                    return true;
                                });
                        } else {
                            MiruInvertedIndex<BM> got = fieldIndexProvider.getFieldIndex(fieldFilter.fieldType).get(
                                fieldId,
                                termComposer.compose(fieldDefinition, term),
                                considerIfIndexIdGreaterThanN);
                            Optional<BM> index = got.getIndex();
                            if (index.isPresent()) {
                                if (filter.operation == MiruFilterOperation.and && bitmaps.isEmpty(index.get())) {
                                    // implicitly empty results, "and" operation would also be empty
                                    solutionLog.log(MiruSolutionLogLevel.DEBUG, "filter: short circuit to 'and' empty bitmap after {} millis.",
                                        System.currentTimeMillis() - start);
                                    return;
                                }
                                fieldBitmaps.add(index.get());
                            }
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
