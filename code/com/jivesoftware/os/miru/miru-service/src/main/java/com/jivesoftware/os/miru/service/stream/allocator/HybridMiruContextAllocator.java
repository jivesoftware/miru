package com.jivesoftware.os.miru.service.stream.allocator;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.jivesoftware.os.filer.chunk.store.ChunkStore;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.chunk.store.FPStripingLocksProvider;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.primative.LongIntKeyValueMarshaller;
import com.jivesoftware.os.filer.keyed.store.KeyedFilerStore;
import com.jivesoftware.os.filer.keyed.store.TxKeyValueStore;
import com.jivesoftware.os.filer.keyed.store.TxKeyedFilerStore;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndexProvider;
import com.jivesoftware.os.miru.plugin.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.service.index.KeyedFilerProvider;
import com.jivesoftware.os.miru.service.index.MiruInternalActivityMarshaller;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import com.jivesoftware.os.miru.service.index.auth.VersionedAuthzExpression;
import com.jivesoftware.os.miru.service.index.disk.MiruFilerActivityIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruFilerAuthzIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruFilerFieldIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruFilerInboxIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruFilerRemovalIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruFilerUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryTimeIndex;
import com.jivesoftware.os.miru.service.locator.MiruHybridResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruResourcePartitionIdentifier;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class HybridMiruContextAllocator implements MiruContextAllocator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruSchemaProvider schemaProvider;
    private final MiruActivityInternExtern activityInternExtern;
    private final MiruReadTrackingWALReader readTrackingWALReader;
    private final MiruHybridResourceLocator hybridResourceLocator;
    private final ByteBufferFactory byteBufferFactory;
    private final int numberOfChunkStores;
    private final int partitionAuthzCacheSize;
    private final boolean partitionDeleteChunkStoreOnClose;
    private final StripingLocksProvider<MiruTermId> fieldIndexStripingLocksProvider;
    private final StripingLocksProvider<MiruStreamId> streamStripingLocksProvider;
    private final StripingLocksProvider<String> authzStripingLocksProvider;
    private final StripingLocksProvider<Long> chunkStripingLocksProvider;
    private final FPStripingLocksProvider fpStripingLocksProvider;

    public HybridMiruContextAllocator(MiruSchemaProvider schemaProvider,
        MiruActivityInternExtern activityInternExtern,
        MiruReadTrackingWALReader readTrackingWALReader,
        MiruHybridResourceLocator hybridResourceLocator,
        ByteBufferFactory byteBufferFactory,
        int numberOfChunkStores,
        int partitionAuthzCacheSize,
        boolean partitionDeleteChunkStoreOnClose,
        StripingLocksProvider<MiruTermId> fieldIndexStripingLocksProvider,
        StripingLocksProvider<MiruStreamId> streamStripingLocksProvider,
        StripingLocksProvider<String> authzStripingLocksProvider,
        StripingLocksProvider<Long> chunkStripingLocksProvider,
        FPStripingLocksProvider fpStripingLocksProvider) {
        this.schemaProvider = schemaProvider;
        this.activityInternExtern = activityInternExtern;
        this.readTrackingWALReader = readTrackingWALReader;
        this.hybridResourceLocator = hybridResourceLocator;
        this.byteBufferFactory = byteBufferFactory;
        this.numberOfChunkStores = numberOfChunkStores;
        this.partitionAuthzCacheSize = partitionAuthzCacheSize;
        this.partitionDeleteChunkStoreOnClose = partitionDeleteChunkStoreOnClose;
        this.fieldIndexStripingLocksProvider = fieldIndexStripingLocksProvider;
        this.streamStripingLocksProvider = streamStripingLocksProvider;
        this.authzStripingLocksProvider = authzStripingLocksProvider;
        this.chunkStripingLocksProvider = chunkStripingLocksProvider;
        this.fpStripingLocksProvider = fpStripingLocksProvider;
    }

    @Override
    public boolean checkMarkedStorage(MiruPartitionCoord coord) throws Exception {
        return true;
    }

    @Override
    public <BM> MiruContext<BM> allocate(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord) throws Exception {
        return allocateRebuilding(bitmaps, coord);
    }

    @Override
    public <BM> MiruContext<BM> stateChanged(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord, MiruContext<BM> from, MiruPartitionState state)
        throws Exception {

        MiruContext<BM> updated = null;
        if (state == MiruPartitionState.online) {
            updated = allocateOnline(bitmaps, coord, from);
            //TODO non-persistent state, EGREGIOUS HACK!
            ((MiruInMemoryTimeIndex) from.timeIndex).transferTo((MiruInMemoryTimeIndex) updated.timeIndex);
        }

        if (updated != null) {
            LOG.info("Updated hybrid context when state changed to {}", state);
            return updated;
        } else {
            return from;
        }
    }

    private <BM> MiruContext<BM> allocateRebuilding(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord) throws Exception {

        // check for schema first
        MiruSchema schema = schemaProvider.getSchema(coord.tenantId);

        ChunkStore[] chunkStores = new ChunkStore[numberOfChunkStores];
        ChunkStoreInitializer chunkStoreInitializer = new ChunkStoreInitializer();
        long initialChunkSize = hybridResourceLocator.getInitialChunkSize();
        for (int i = 0; i < numberOfChunkStores; i++) {
            chunkStores[i] = chunkStoreInitializer.create(byteBufferFactory, initialChunkSize, chunkStripingLocksProvider);
        }

        return buildMiruContext(bitmaps, schema, chunkStores);
    }

    private <BM> MiruContext<BM> allocateOnline(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord, MiruContext<BM> from) throws Exception {

        MiruSchema schema = from.schema;

        MiruResourcePartitionIdentifier identifier = hybridResourceLocator.acquire();

        ChunkStore[] fromChunkStores = from.chunkStores.get();

        File[] chunkDirs = hybridResourceLocator.getChunkDirectories(identifier, "chunks");
        StripingLocksProvider<Long> locksProvider = new StripingLocksProvider<>(64);
        ChunkStore[] chunkStores = new ChunkStore[numberOfChunkStores];
        ChunkStoreInitializer chunkStoreInitializer = new ChunkStoreInitializer();
        for (int i = 0; i < numberOfChunkStores; i++) {
            chunkStores[i] = chunkStoreInitializer.openOrCreate(
                chunkDirs,
                Math.abs(coord.hashCode() + i) % chunkDirs.length,
                "chunk-" + i,
                4_096, //TODO configure?
                locksProvider);
            fromChunkStores[i].copyTo(chunkStores[i]);
        }

        return buildMiruContext(bitmaps, schema, chunkStores);
    }

    private <BM> MiruContext<BM> buildMiruContext(MiruBitmaps<BM> bitmaps, MiruSchema schema, ChunkStore[] chunkStores)
        throws Exception {

        MiruInMemoryTimeIndex timeIndex = new MiruInMemoryTimeIndex(
            Optional.<MiruInMemoryTimeIndex.TimeOrderAnomalyStream>absent(),
            new TxKeyValueStore<>(chunkStores,
                new LongIntKeyValueMarshaller(),
                keyBytes("timeIndex"),
                8, false, 4, false));

        TxKeyedFilerStore activityKeyedFilerStore = new TxKeyedFilerStore(chunkStores, keyBytes("activityIndex-map"), fpStripingLocksProvider);
        MiruFilerActivityIndex activityIndex = new MiruFilerActivityIndex(
            activityKeyedFilerStore,
            new MiruInternalActivityMarshaller(),
            new KeyedFilerProvider(activityKeyedFilerStore, keyBytes("activityIndex-size")));

        @SuppressWarnings("unchecked")
        MiruFilerFieldIndex<BM>[] fieldIndexes = new MiruFilerFieldIndex[MiruFieldType.values().length];
        for (MiruFieldType fieldType : MiruFieldType.values()) {
            KeyedFilerStore[] indexes = new KeyedFilerStore[schema.fieldCount()];
            for (MiruFieldDefinition fieldDefinition : schema.getFieldDefinitions()) {
                int fieldId = fieldDefinition.fieldId;
                if (fieldType == MiruFieldType.latest && !fieldDefinition.indexLatest
                    || fieldType == MiruFieldType.pairedLatest && fieldDefinition.pairedLatestFieldNames.isEmpty()
                    || fieldType == MiruFieldType.bloom && fieldDefinition.bloomFieldNames.isEmpty()) {
                    indexes[fieldId] = null;
                } else {
                    indexes[fieldId] = new TxKeyedFilerStore(chunkStores, keyBytes("field-" + fieldType.name() + "-" + fieldId), fpStripingLocksProvider);
                }
            }
            fieldIndexes[fieldType.getIndex()] = new MiruFilerFieldIndex<>(bitmaps, indexes, fieldIndexStripingLocksProvider);
        }
        MiruFieldIndexProvider<BM> fieldIndexProvider = new MiruFieldIndexProvider<>(fieldIndexes);

        MiruAuthzUtils<BM> authzUtils = new MiruAuthzUtils<>(bitmaps);

        //TODO share the cache?
        Cache<VersionedAuthzExpression, BM> authzCache = CacheBuilder.newBuilder()
            .maximumSize(partitionAuthzCacheSize)
            .expireAfterAccess(1, TimeUnit.MINUTES) //TODO should be adjusted with respect to tuning GC (prevent promotion from eden space)
            .build();

        MiruFilerAuthzIndex<BM> authzIndex = new MiruFilerAuthzIndex<>(
            bitmaps,
            new TxKeyedFilerStore(chunkStores, keyBytes("authzIndex"), fpStripingLocksProvider),
            new MiruAuthzCache<>(bitmaps, authzCache, activityInternExtern, authzUtils),
            authzStripingLocksProvider);

        MiruFilerRemovalIndex<BM> removalIndex = new MiruFilerRemovalIndex<>(
            bitmaps,
            new TxKeyedFilerStore(chunkStores, keyBytes("removalIndex"), fpStripingLocksProvider),
            new byte[] { 0 },
            -1,
            new Object());

        MiruFilerUnreadTrackingIndex<BM> unreadTrackingIndex = new MiruFilerUnreadTrackingIndex<>(
            bitmaps,
            new TxKeyedFilerStore(chunkStores, keyBytes("unreadTrackingIndex"), fpStripingLocksProvider),
            streamStripingLocksProvider);

        MiruFilerInboxIndex<BM> inboxIndex = new MiruFilerInboxIndex<>(bitmaps,
            new TxKeyedFilerStore(chunkStores, keyBytes("inboxIndex"), fpStripingLocksProvider),
            streamStripingLocksProvider);

        StripingLocksProvider<MiruStreamId> streamLocks = new StripingLocksProvider<>(64);

        return new MiruContext<>(schema,
            timeIndex,
            activityIndex,
            fieldIndexProvider,
            authzIndex,
            removalIndex,
            unreadTrackingIndex,
            inboxIndex,
            readTrackingWALReader,
            activityInternExtern,
            streamLocks,
            Optional.of(chunkStores),
            Optional.<MiruResourcePartitionIdentifier>absent());
    }

    @Override
    public <BM> void close(MiruContext<BM> context) {
        if (context.chunkStores.isPresent() && partitionDeleteChunkStoreOnClose) {
            ChunkStore[] chunkStores = context.chunkStores.get();
            for (ChunkStore chunkStore : chunkStores) {
                try {
                    chunkStore.delete();
                } catch (IOException e) {
                    LOG.warn("Failed to delete chunk store", e);
                }
            }
        }
    }

    private byte[] keyBytes(String key) {
        return key.getBytes(Charsets.UTF_8);
    }

    private String[] filesToPaths(File[] files) {
        String[] paths = new String[files.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = files[i].getAbsolutePath();
        }
        return paths;
    }

}
