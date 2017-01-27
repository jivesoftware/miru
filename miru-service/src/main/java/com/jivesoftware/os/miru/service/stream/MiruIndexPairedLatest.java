package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruIndexUtil;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 *
 */
public class MiruIndexPairedLatest<BM extends IBM, IBM> {

    private final static MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruIndexUtil indexUtil = new MiruIndexUtil();

    // Answers the question,
    // "For each distinct value of this field, what is the latest activity against each distinct value of the related field?"
    public List<Future<List<PairedLatestWork>>> compose(MiruContext<BM, IBM, ?> context,
        final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds,
        ExecutorService indexExecutor)
        throws Exception {

        List<MiruFieldDefinition> fieldsWithAggregates = context.getSchema().getFieldsWithPairedLatest();
        List<Future<List<PairedLatestWork>>> workFutures = Lists.newArrayList();
        for (final MiruFieldDefinition fieldDefinition : fieldsWithAggregates) {
            List<MiruFieldDefinition> aggregateFieldDefinitions = context.getSchema().getPairedLatestFieldDefinitions(fieldDefinition.fieldId);
            for (final MiruFieldDefinition aggregateFieldDefinition : aggregateFieldDefinitions) {
                workFutures.add(indexExecutor.submit(() -> {
                    Map<MiruTermId, List<IdAndTerm>> fieldWork = Maps.newHashMap();
                    Set<WriteAggregateKey> visited = Sets.newHashSet();

                    // walk backwards so we see the largest id first, and mark visitors for each coordinate
                    for (int i = internalActivityAndIds.size() - 1; i >= 0; i--) {
                        final MiruActivityAndId<MiruInternalActivity> internalActivityAndId = internalActivityAndIds.get(i);
                        MiruInternalActivity activity = internalActivityAndId.activity;
                        MiruTermId[] fieldValues = activity.fieldsValues[fieldDefinition.fieldId];
                        if (fieldValues != null && fieldValues.length > 0) {
                            MiruTermId[] aggregateFieldValues = activity.fieldsValues[aggregateFieldDefinition.fieldId];
                            if (aggregateFieldValues != null && aggregateFieldValues.length > 0) {
                                for (final MiruTermId fieldValue : fieldValues) {
                                    for (MiruTermId aggregateFieldValue : aggregateFieldValues) {
                                        WriteAggregateKey key = new WriteAggregateKey(fieldValue, aggregateFieldValue);
                                        if (visited.add(key)) {
                                            List<IdAndTerm> idAndTerms = fieldWork.get(fieldValue);
                                            if (idAndTerms == null) {
                                                idAndTerms = Lists.newArrayList();
                                                fieldWork.put(fieldValue, idAndTerms);
                                            }
                                            idAndTerms.add(new IdAndTerm(internalActivityAndId.id, aggregateFieldValue));
                                        }
                                    }
                                }
                            }
                        }
                    }
                    List<PairedLatestWork> workList = Lists.newArrayListWithCapacity(fieldWork.size());
                    for (Map.Entry<MiruTermId, List<IdAndTerm>> entry : fieldWork.entrySet()) {
                        workList.add(new PairedLatestWork(fieldDefinition.fieldId, aggregateFieldDefinition.fieldId, entry.getKey(), entry.getValue()));
                    }
                    return workList;
                }));
            }
        }
        return workFutures;
    }

    public Future<List<PairedLatestWork>> prepare(final MiruContext<BM, IBM, ?> context,
        final List<Future<List<PairedLatestWork>>> pairedLatestWorkFutures,
        ExecutorService indexExecutor) throws Exception {

        return indexExecutor.submit(() -> {
            List<PairedLatestWork> pairedLatestWork = Lists.newArrayList();
            for (Future<List<PairedLatestWork>> future : pairedLatestWorkFutures) {
                pairedLatestWork.addAll(future.get());
            }
            Collections.sort(pairedLatestWork);
            return pairedLatestWork;
        });
    }

    public List<Future<?>> index(final MiruContext<BM, IBM, ?> context,
        MiruTenantId tenantId, Future<List<PairedLatestWork>> pairedLatestWorksFuture,
        final boolean repair,
        ExecutorService indexExecutor)
        throws Exception {

        List<PairedLatestWork> pairedLatestWorks = pairedLatestWorksFuture.get();

        final MiruFieldIndex<BM, IBM> allFieldIndex = context.fieldIndexProvider.getFieldIndex(MiruFieldType.primary);
        final MiruFieldIndex<BM, IBM> pairedLatestFieldIndex = context.fieldIndexProvider.getFieldIndex(MiruFieldType.pairedLatest);
        int callableCount = 0;
        List<Future<?>> futures = Lists.newArrayListWithCapacity(pairedLatestWorks.size());
        for (final PairedLatestWork pairedLatestWork : pairedLatestWorks) {
            futures.add(indexExecutor.submit(() -> {
                StackBuffer stackBuffer = new StackBuffer();
                MiruTermId fieldValue = pairedLatestWork.fieldValue;
                List<IdAndTerm> idAndTerms = pairedLatestWork.work;

                MiruFieldDefinition aggregateFieldDefinition = context.getSchema().getFieldDefinition(pairedLatestWork.aggregateFieldId);
                MiruTermId pairedLatestTerm = indexUtil.makePairedLatestTerm(fieldValue, aggregateFieldDefinition.name);
                MiruInvertedIndex<BM, IBM> invertedIndex = pairedLatestFieldIndex
                    .getOrCreateInvertedIndex("indexPairedLatest", pairedLatestWork.fieldId, pairedLatestTerm);

                List<IBM> aggregateBitmaps = Lists.newArrayListWithCapacity(idAndTerms.size());
                TIntList ids = new TIntArrayList(idAndTerms.size());
                BitmapAndLastId<BM> container = new BitmapAndLastId<>();
                for (IdAndTerm idAndTerm : idAndTerms) {
                    MiruTermId aggregateFieldValue = idAndTerm.term;
                    MiruInvertedIndex<BM, IBM> aggregateInvertedIndex = allFieldIndex.getOrCreateInvertedIndex("indexPairedLatest",
                        pairedLatestWork.aggregateFieldId, aggregateFieldValue);
                    container.clear();
                    aggregateInvertedIndex.getIndex(container, stackBuffer);
                    if (container.isSet()) {
                        aggregateBitmaps.add(container.getBitmap());
                        ids.add(idAndTerm.id);
                    }
                }

                log.inc("count>andNot", aggregateBitmaps.size());
                log.inc("count>andNot", aggregateBitmaps.size(), tenantId.toString());
                invertedIndex.andNotToSourceSize(aggregateBitmaps, stackBuffer);

                ids.reverse(); // we built in reverse order, so flip back to ascending
                log.inc("count>set", 1);
                log.inc("count>set", 1, tenantId.toString());
                pairedLatestFieldIndex.set(pairedLatestWork.fieldId, pairedLatestTerm, ids.toArray(), null, stackBuffer);

                return null;
            }));
            callableCount++;
        }

        log.trace("Submitted {} aggregate field callables", callableCount);

        return futures;
    }
}
