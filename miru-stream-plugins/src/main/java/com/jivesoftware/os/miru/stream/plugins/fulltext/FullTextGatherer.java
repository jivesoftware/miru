package com.jivesoftware.os.miru.stream.plugins.fulltext;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndex.CustomMarshaller;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.partition.MiruQueryablePartition;
import com.jivesoftware.os.miru.plugin.plugin.IndexCloseCallback;
import com.jivesoftware.os.miru.plugin.plugin.IndexCommitCallback;
import com.jivesoftware.os.miru.plugin.plugin.IndexOpenCallback;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class FullTextGatherer implements IndexOpenCallback, IndexCommitCallback, IndexCloseCallback {

    private static final byte[] KEY = "fullText".getBytes(StandardCharsets.UTF_8);

    private static final CustomMarshaller<Integer> MARSHALLER = new CustomMarshaller<Integer>() {
        @Override
        public byte[] toBytes(Integer comparable) {
            return FilerIO.intBytes(comparable);
        }

        @Override
        public Integer fromBytes(byte[] bytes) {
            return FilerIO.bytesInt(bytes);
        }
    };

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruProvider<? extends Miru> miruProvider;
    private final FullTextTermProviders fullTextTermProviders;
    private final int batchSize;
    private final ScheduledExecutorService executorService;

    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();
    private final Map<MiruPartitionCoord, Gatherer> gatherers = Maps.newConcurrentMap();

    public FullTextGatherer(MiruProvider<? extends Miru> miruProvider,
        FullTextTermProviders fullTextTermProviders,
        int batchSize,
        ScheduledExecutorService executorService) {
        this.miruProvider = miruProvider;
        this.fullTextTermProviders = fullTextTermProviders;
        this.batchSize = batchSize;
        this.executorService = executorService;
    }

    @Override
    public void indexOpen(MiruPartitionCoord coord) {
        indexCommit(coord);
    }

    @Override
    public void indexCommit(MiruPartitionCoord coord) {
        Gatherer gatherer = gatherers.computeIfAbsent(coord, Gatherer::new);
        gatherer.indexCommit();
    }

    @Override
    public boolean canClose(MiruPartitionCoord coord) {
        Gatherer gatherer = gatherers.get(coord);
        return gatherer == null || !gatherer.running.get();
    }

    @Override
    public void indexClose(MiruPartitionCoord coord) {
        Gatherer gatherer = gatherers.remove(coord);
        if (gatherer != null) {
            gatherer.close();
        }
    }

    private class Gatherer implements Runnable {

        private final MiruPartitionCoord coord;

        private final AtomicBoolean running = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicLong commitVersion = new AtomicLong();

        private Gatherer(MiruPartitionCoord coord) {
            this.coord = coord;
        }

        private void indexCommit() {
            synchronized (this) {
                commitVersion.incrementAndGet();
                if (running.compareAndSet(false, true)) {
                    executorService.submit(this);
                }
            }
        }

        private void close() {
            synchronized (this) {
                closed.set(true);
            }
        }

        @Override
        public void run() {
            try {
                if (closed.get()) {
                    running.set(false);
                    return;
                }
                long version = commitVersion.get();
                Miru miru = miruProvider.getMiru(coord.tenantId);
                Optional<? extends MiruQueryablePartition<?, ?>> got = miru.getQueryablePartition(coord);
                if (got.isPresent()) {
                    MiruQueryablePartition<?, ?> queryablePartition = got.get();
                    if (queryablePartition.isAvailable()) {
                        try (MiruRequestHandle<?, ?, ?> handle = queryablePartition.acquireQueryHandle()) {
                            gather(handle);
                            synchronized (this) {
                                long current = commitVersion.get();
                                if (current == version || closed.get()) {
                                    running.set(false);
                                } else {
                                    executorService.submit(this);
                                }
                            }
                        }
                    } else {
                        LOG.info("Delaying gather for {} because partition is unavailable", coord);
                        executorService.schedule(this, 10_000L, TimeUnit.MILLISECONDS); //TODO config
                    }
                } else {
                    LOG.warn("Could not find queryable partition for coord:{}", coord);
                    running.set(false);
                }
            } catch (SchemaNotReadyException e) {
                LOG.error("Schema not ready coord:{} reason: {}", coord, e.getMessage());
                executorService.schedule(this, 60_000L, TimeUnit.MILLISECONDS); //TODO config
            } catch (Throwable t) {
                LOG.error("Failed to gather full text for coord:{}", new Object[] { coord }, t);
                executorService.schedule(this, 10_000L, TimeUnit.MILLISECONDS); //TODO config
            }
        }
    }

    private <BM extends IBM, IBM> void gather(MiruRequestHandle<BM, IBM, ?> handle) throws Exception {

        MiruRequestContext<BM, IBM, ?> requestContext = handle.getRequestContext();
        MiruSchema schema = requestContext.getSchema();
        MiruPartitionCoord coord = handle.getCoord();

        FullTextTermProvider fullTextTermProvider = fullTextTermProviders.get(schema.getName());
        if (fullTextTermProvider == null || !fullTextTermProvider.isEnabled(coord.tenantId)) {
            return;
        }

        for (String fieldName : fullTextTermProvider.getFieldNames()) {
            if (schema.getFieldId(fieldName) < 0) {
                throw new SchemaNotReadyException("Unknown gather field: " + fieldName);
            }
        }
        for (String fieldName : fullTextTermProvider.getIndexFieldNames()) {
            if (schema.getFieldId(fieldName) < 0) {
                throw new SchemaNotReadyException("Unknown index field: " + fieldName);
            }
        }

        MiruFilter acceptableFilter = fullTextTermProvider.getAcceptableFilter();
        MiruSolutionLog solutionLog = new MiruSolutionLog(MiruSolutionLogLevel.NONE);

        MiruBitmaps<BM, IBM> bitmaps = handle.getBitmaps();

        StackBuffer stackBuffer = new StackBuffer();
        MiruActivityIndex activityIndex = requestContext.getActivityIndex();

        Integer fullTextId = requestContext.getSipIndex().getCustom(KEY, MARSHALLER);
        int start = (fullTextId == null) ? 0 : fullTextId + 1;
        int lastId = activityIndex.lastId(stackBuffer);

        IBM acceptable = bitmaps.buildIndexMask(start, lastId, requestContext.getRemovalIndex(), null, stackBuffer);
        if (!MiruFilter.NO_FILTER.equals(acceptableFilter)) {
            BM filtered = aggregateUtil.filter("fullTextGatherer", bitmaps, requestContext, acceptableFilter, solutionLog, null, lastId, start - 1, -1,
                stackBuffer);
            acceptable = bitmaps.and(Arrays.asList(acceptable, filtered));
        }

        MiruIntIterator iter = bitmaps.intIterator(acceptable);
        int[] batch = new int[batchSize];
        while (iter.hasNext()) {
            int count = 0;
            int toId = -1;
            for (int i = 0; i < batchSize && iter.hasNext(); i++) {
                toId = iter.next();
                batch[i] = toId;
                count++;
            }

            int[] indexes;
            if (count < batchSize) {
                indexes = new int[count];
                System.arraycopy(batch, 0, indexes, 0, count);
            } else {
                indexes = batch;
            }

            gatherBatch(coord,
                requestContext,
                fullTextTermProvider,
                indexes,
                stackBuffer);
            requestContext.getSipIndex().setCustom(KEY, toId, MARSHALLER);
            LOG.info("Gathered {} full text items for coord:{} to:{}", count, coord, toId);
        }
    }

    private <BM extends IBM, IBM> void gatherBatch(MiruPartitionCoord coord,
        MiruRequestContext<BM, IBM, ?> requestContext,
        FullTextTermProvider fullTextTermProvider,
        int[] indexes,
        StackBuffer stackBuffer) throws Exception {

        MiruFieldIndex<BM, IBM> primaryFieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);
        MiruActivityIndex activityIndex = requestContext.getActivityIndex();
        MiruTermComposer termComposer = requestContext.getTermComposer();
        MiruSchema schema = requestContext.getSchema();

        String[] gatherFieldNames = fullTextTermProvider.getFieldNames();

        Map<String, MiruValue[][]> termIds = Maps.newHashMap();
        for (int i = 0; i < gatherFieldNames.length; i++) {
            String fieldName = gatherFieldNames[i];
            int fieldId = schema.getFieldId(fieldName);
            if (fieldId < 0) {
                throw new RuntimeException("Unknown field: " + fieldName);
            }
            MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(fieldId);
            MiruTermId[][] got = activityIndex.getAll("fullTextGatherer", indexes, fieldId, stackBuffer);
            MiruValue[][] values = new MiruValue[got.length][];
            for (int j = 0; j < got.length; j++) {
                values[j] = new MiruValue[got[j].length];
                for (int k = 0; k < got[j].length; k++) {
                    values[j][k] = new MiruValue(termComposer.decompose(schema, fieldDefinition, stackBuffer, got[j][k]));
                }
            }
            termIds.put(fieldName, values);
        }

        boolean result = fullTextTermProvider.gatherText(coord, indexes, termIds, (fieldName, value, ids) -> {
            int fieldId = schema.getFieldId(fieldName);
            if (fieldId < 0) {
                throw new RuntimeException("Unknown field: " + fieldName);
            }
            MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(fieldId);
            MiruTermId termId = termComposer.compose(schema, fieldDefinition, stackBuffer, value.parts);
            MiruInvertedIndex<BM, IBM> toIndex = primaryFieldIndex.get("fullTextGatherer", fieldId, termId);
            toIndex.set(stackBuffer, ids);
            return true;
        });

        if (!result) {
            throw new RuntimeException("Term provider returned a failure result");
        }
    }

    private static class SchemaNotReadyException extends Exception {

        public SchemaNotReadyException(String message) {
            super(message);
        }
    }
}
