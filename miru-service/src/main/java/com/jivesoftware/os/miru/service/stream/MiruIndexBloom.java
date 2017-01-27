package com.jivesoftware.os.miru.service.stream;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.plugin.index.BloomIndex;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruIndexUtil;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 *
 */
public class MiruIndexBloom<BM extends IBM, IBM> {

    private final static MetricLogger log = MetricLoggerFactory.getLogger();

    private final BloomIndex<BM, IBM> bloomIndex;
    private final MiruIndexUtil indexUtil = new MiruIndexUtil();

    public MiruIndexBloom(BloomIndex<BM, IBM> bloomIndex) {
        this.bloomIndex = bloomIndex;
        //this.bloomIndex = new BloomIndex<>(bitmaps, Hashing.murmur3_128(), 100_000, 0.01f); // TODO fix somehow
    }

    public List<Future<List<BloomWork>>> compose(MiruContext<BM, IBM, ?> context,
        final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds,
        ExecutorService indexExecutor)
        throws Exception {

        List<MiruFieldDefinition> fieldsWithBlooms = context.getSchema().getFieldsWithBloom();
        List<Future<List<BloomWork>>> workFutures = Lists.newArrayList();
        for (final MiruFieldDefinition fieldDefinition : fieldsWithBlooms) {
            List<MiruFieldDefinition> bloominFieldDefinitions = context.getSchema().getBloomFieldDefinitions(fieldDefinition.fieldId);
            for (final MiruFieldDefinition bloominFieldDefinition : bloominFieldDefinitions) {
                workFutures.add(indexExecutor.submit(() -> {
                    Map<MiruTermId, List<MiruTermId>> fieldValueWork = Maps.newHashMap();
                    for (MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
                        MiruInternalActivity activity = internalActivityAndId.activity;
                        MiruTermId[] fieldValues = activity.fieldsValues[fieldDefinition.fieldId];
                        if (fieldValues != null && fieldValues.length > 0) {
                            final MiruTermId[] bloomFieldValues = activity.fieldsValues[bloominFieldDefinition.fieldId];
                            if (bloomFieldValues != null && bloomFieldValues.length > 0) {
                                for (final MiruTermId fieldValue : fieldValues) {
                                    List<MiruTermId> combinedBloomFieldValues = fieldValueWork.get(fieldValue);
                                    if (combinedBloomFieldValues == null) {
                                        combinedBloomFieldValues = Lists.newArrayList();
                                        fieldValueWork.put(fieldValue, combinedBloomFieldValues);
                                    }
                                    Collections.addAll(combinedBloomFieldValues, bloomFieldValues);
                                }
                            }
                        }
                    }
                    List<BloomWork> workList = Lists.newArrayListWithCapacity(fieldValueWork.size());
                    for (Map.Entry<MiruTermId, List<MiruTermId>> entry : fieldValueWork.entrySet()) {
                        workList.add(new BloomWork(fieldDefinition.fieldId, bloominFieldDefinition.fieldId, entry.getKey(), entry.getValue()));
                    }
                    return workList;
                }));
            }
        }
        return workFutures;
    }

    public Future<List<BloomWork>> prepare(final MiruContext<BM, IBM, ?> context,
        final List<Future<List<BloomWork>>> bloomWorkFutures,
        ExecutorService indexExecutor)
        throws ExecutionException, InterruptedException {

        return indexExecutor.submit(() -> {
            List<BloomWork> bloomWorks = Lists.newArrayList();
            for (Future<List<BloomWork>> future : bloomWorkFutures) {
                bloomWorks.addAll(future.get());
            }
            Collections.sort(bloomWorks);
            return bloomWorks;
        });
    }

    public List<Future<?>> index(final MiruContext<BM, IBM, ?> context,
        MiruTenantId tenantId, final Future<List<BloomWork>> bloomWorksFuture,
        boolean repair,
        ExecutorService indexExecutor)
        throws ExecutionException, InterruptedException {
        StackBuffer stackBuffer = new StackBuffer();
        List<BloomWork> bloomWorks = bloomWorksFuture.get();

        final MiruFieldIndex<BM, IBM> bloomFieldIndex = context.getFieldIndexProvider().getFieldIndex(MiruFieldType.bloom);
        int callableCount = 0;
        List<Future<?>> futures = Lists.newArrayList();
        for (final BloomWork bloomWork : bloomWorks) {
            futures.add(indexExecutor.submit(() -> {
                log.inc("count", bloomWork.bloomFieldValues.size());
                log.inc("count", bloomWork.bloomFieldValues.size(), tenantId.toString());
                MiruFieldDefinition bloomFieldDefinition = context.getSchema().getFieldDefinition(bloomWork.bloomFieldId);
                MiruTermId compositeBloomId = indexUtil.makeBloomTerm(bloomWork.fieldValue, bloomFieldDefinition.name);
                MiruInvertedIndex<BM, IBM> invertedIndex = bloomFieldIndex.getOrCreateInvertedIndex("indexBloom", bloomWork.fieldId, compositeBloomId);
                bloomIndex.put(invertedIndex, bloomWork.bloomFieldValues, stackBuffer);
                return null;
            }));
            callableCount++;
        }
        log.trace("Submitted {} bloom callables", callableCount);

        return futures;
    }

}
