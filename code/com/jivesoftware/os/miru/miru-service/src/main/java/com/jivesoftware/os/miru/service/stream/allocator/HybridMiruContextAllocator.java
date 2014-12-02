package com.jivesoftware.os.miru.service.stream.allocator;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.ByteBufferProvider;
import com.jivesoftware.os.filer.keyed.store.PartitionedMapChunkBackedKeyedStore;
import com.jivesoftware.os.filer.keyed.store.VariableKeySizeMapChunkBackedKeyedStore;
import com.jivesoftware.os.filer.map.store.ByteBufferProviderBackedMapChunkFactory;
import com.jivesoftware.os.filer.map.store.FileBackedMapChunkFactory;
import com.jivesoftware.os.filer.map.store.MapChunkFactory;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndexProvider;
import com.jivesoftware.os.miru.plugin.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.service.index.MiruInternalActivityMarshaller;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import com.jivesoftware.os.miru.service.index.auth.VersionedAuthzExpression;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskFieldIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruHybridActivityIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryAuthzIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryInboxIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryRemovalIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryTimeIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.locator.MiruHybridResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruResourcePartitionIdentifier;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class HybridMiruContextAllocator implements MiruContextAllocator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    //TODO push to schema
    private static final int[] IN_MEMORY_FIELD_KEY_SIZE_THRESHOLDS = new int[]{2, 4, 6, 8, 10, 12, 16, 64, 256, 1_024};
    private static final int[] ON_DISK_FIELD_KEY_SIZE_THRESHOLDS = new int[]{4, 16, 64, 256, 1_024};
    //private static final int[] IN_MEMORY_FIELD_KEY_SIZE_PARTITIONS = new int[] { 1, 2, 2, 6, 8, 6, 2, 1, 1, 1 };

    private final MiruSchemaProvider schemaProvider;
    private final MiruActivityInternExtern activityInternExtern;
    private final MiruReadTrackingWALReader readTrackingWALReader;
    private final MiruHybridResourceLocator hybridResourceLocator;
    private final ByteBufferFactory byteBufferFactory;
    private final int numberOfChunkStores;
    private final int partitionAuthzCacheSize;
    private final boolean partitionDeleteChunkStoreOnClose;
    private final int partitionChunkStoreConcurrencyLevel;
    private final int partitionChunkStoreStripingLevel;

    public HybridMiruContextAllocator(MiruSchemaProvider schemaProvider,
        MiruActivityInternExtern activityInternExtern,
        MiruReadTrackingWALReader readTrackingWALReader,
        MiruHybridResourceLocator hybridResourceLocator,
        ByteBufferFactory byteBufferFactory,
        int numberOfChunkStores,
        int partitionAuthzCacheSize,
        boolean partitionDeleteChunkStoreOnClose,
        int partitionChunkStoreConcurrencyLevel,
        int partitionChunkStoreStripingLevel) {
        this.schemaProvider = schemaProvider;
        this.activityInternExtern = activityInternExtern;
        this.readTrackingWALReader = readTrackingWALReader;
        this.hybridResourceLocator = hybridResourceLocator;
        this.byteBufferFactory = byteBufferFactory;
        this.numberOfChunkStores = numberOfChunkStores;
        this.partitionAuthzCacheSize = partitionAuthzCacheSize;
        this.partitionDeleteChunkStoreOnClose = partitionDeleteChunkStoreOnClose;
        this.partitionChunkStoreConcurrencyLevel = partitionChunkStoreConcurrencyLevel;
        this.partitionChunkStoreStripingLevel = partitionChunkStoreStripingLevel;
    }

    @Override
    public boolean checkMarkedStorage(MiruPartitionCoord coord) throws Exception {
        return true;
    }

    @Override
    public <BM> MiruContext<BM> allocate(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord) throws Exception {
        // check for schema first
        MiruSchema schema = schemaProvider.getSchema(coord.tenantId);

        MiruInMemoryTimeIndex timeIndex = new MiruInMemoryTimeIndex(Optional.<MiruInMemoryTimeIndex.TimeOrderAnomalyStream>absent(),
            new ByteBufferProviderBackedMapChunkFactory(8, false, 4, false, 32, new ByteBufferProvider("timeIndex", byteBufferFactory)));

        MultiChunkStore multiChunkStore = new ChunkStoreInitializer().initializeMultiByteBufferBacked(
            "chunks", byteBufferFactory, numberOfChunkStores, hybridResourceLocator.getInitialChunkSize(), true,
            partitionChunkStoreConcurrencyLevel, partitionChunkStoreStripingLevel);

        MapChunkFactory activityMapChunkFactory = new ByteBufferProviderBackedMapChunkFactory(4, false, 8, false, 100,
            new ByteBufferProvider("activityIndex-map", byteBufferFactory));
        MapChunkFactory activitySwapChunkFactory = new ByteBufferProviderBackedMapChunkFactory(4, false, 8, false, 100,
            new ByteBufferProvider("activityIndex-swap", byteBufferFactory));
        MiruHybridActivityIndex activityIndex = new MiruHybridActivityIndex(
            new PartitionedMapChunkBackedKeyedStore(activityMapChunkFactory, activitySwapChunkFactory, multiChunkStore, 24),
            new MiruInternalActivityMarshaller());

        @SuppressWarnings("unchecked")
        MiruOnDiskFieldIndex<BM>[] fieldIndexes = new MiruOnDiskFieldIndex[MiruFieldType.values().length];
        for (MiruFieldType fieldType : MiruFieldType.values()) {
            VariableKeySizeMapChunkBackedKeyedStore[] indexes = new VariableKeySizeMapChunkBackedKeyedStore[schema.fieldCount()];
            for (int fieldId : schema.getFieldIds()) {
                //TODO expose to config
                VariableKeySizeMapChunkBackedKeyedStore.Builder builder = new VariableKeySizeMapChunkBackedKeyedStore.Builder();

                for (int keySize : ON_DISK_FIELD_KEY_SIZE_THRESHOLDS) {
                    String key = fieldType.name() + "-fieldId-" + fieldId + "-keySize-" + keySize;
                    builder.add(keySize, new PartitionedMapChunkBackedKeyedStore(
                        new ByteBufferProviderBackedMapChunkFactory(keySize, true, 8, false, 100, new ByteBufferProvider(key + "-map", byteBufferFactory)),
                        new ByteBufferProviderBackedMapChunkFactory(keySize, true, 8, false, 100, new ByteBufferProvider(key + "-swap", byteBufferFactory)),
                        multiChunkStore,
                        4)); //TODO expose number of partitions
                }

                indexes[fieldId] = builder.build();
            }
            fieldIndexes[fieldType.getIndex()] = new MiruOnDiskFieldIndex<>(bitmaps, indexes);
        }
        MiruFieldIndexProvider<BM> fieldIndexProvider = new MiruFieldIndexProvider<>(fieldIndexes);

        MiruAuthzUtils<BM> authzUtils = new MiruAuthzUtils<>(bitmaps);

        //TODO share the cache?
        Cache<VersionedAuthzExpression, BM> authzCache = CacheBuilder.newBuilder()
            .maximumSize(partitionAuthzCacheSize)
            .expireAfterAccess(1, TimeUnit.MINUTES) //TODO should be adjusted with respect to tuning GC (prevent promotion from eden space)
            .build();
        MiruInMemoryAuthzIndex<BM> authzIndex = new MiruInMemoryAuthzIndex<>(
            bitmaps, new MiruAuthzCache<>(bitmaps, authzCache, activityInternExtern, authzUtils));

        MiruInMemoryRemovalIndex<BM> removalIndex = new MiruInMemoryRemovalIndex<>(bitmaps);

        MiruInMemoryUnreadTrackingIndex<BM> unreadTrackingIndex = new MiruInMemoryUnreadTrackingIndex<>(bitmaps);

        MiruInMemoryInboxIndex<BM> inboxIndex = new MiruInMemoryInboxIndex<>(bitmaps);

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
            Optional.of(multiChunkStore),
            Optional.<MiruResourcePartitionIdentifier>absent());
    }

    @Override
    public <BM> MiruContext<BM> stateChanged(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord, MiruContext<BM> from, MiruPartitionState state)
        throws Exception {

        if (state != MiruPartitionState.online) {
            return from;
        }

        MiruSchema schema = from.schema;

        MiruResourcePartitionIdentifier identifier = hybridResourceLocator.acquire();

        MultiChunkStore fromMultiChunkStore = from.chunkStore.get();
        MultiChunkStore multiChunkStore = new ChunkStoreInitializer().copyToMultiFileBacked(
            fromMultiChunkStore,
            filesToPaths(hybridResourceLocator.getChunkDirectories(identifier, "chunk")),
            "stream",
            true,
            partitionChunkStoreConcurrencyLevel,
            partitionChunkStoreStripingLevel);

        MiruHybridActivityIndex activityIndex = new MiruHybridActivityIndex(
            new PartitionedMapChunkBackedKeyedStore(
                new FileBackedMapChunkFactory(4, false, 8, false, 100,
                    filesToPaths(hybridResourceLocator.getMapDirectories(identifier, "activity"))),
                new FileBackedMapChunkFactory(4, false, 8, false, 100,
                    filesToPaths(hybridResourceLocator.getSwapDirectories(identifier, "activity"))),
                multiChunkStore, 24),
            new MiruInternalActivityMarshaller());
        ((MiruHybridActivityIndex) from.activityIndex).copyTo(activityIndex);

        File[] baseIndexMapDirectories = hybridResourceLocator.getMapDirectories(identifier, "index");
        File[] baseIndexSwapDirectories = hybridResourceLocator.getSwapDirectories(identifier, "index");

        @SuppressWarnings("unchecked")
        MiruOnDiskFieldIndex<BM>[] fieldIndexes = new MiruOnDiskFieldIndex[MiruFieldType.values().length];
        for (MiruFieldType fieldType : MiruFieldType.values()) {
            VariableKeySizeMapChunkBackedKeyedStore[] indexes = new VariableKeySizeMapChunkBackedKeyedStore[schema.fieldCount()];
            for (int fieldId : schema.getFieldIds()) {
                //TODO expose to config
                VariableKeySizeMapChunkBackedKeyedStore.Builder builder = new VariableKeySizeMapChunkBackedKeyedStore.Builder();

                for (int keySize : ON_DISK_FIELD_KEY_SIZE_THRESHOLDS) {
                    String fieldTypeAndId = fieldType.name() + "-" + fieldId;
                    String[] mapDirectories = new String[baseIndexMapDirectories.length];
                    for (int i = 0; i < mapDirectories.length; i++) {
                        mapDirectories[i] = new File(new File(baseIndexMapDirectories[i], fieldTypeAndId), String.valueOf(keySize)).getAbsolutePath();
                    }
                    String[] swapDirectories = new String[baseIndexSwapDirectories.length];
                    for (int i = 0; i < swapDirectories.length; i++) {
                        swapDirectories[i] = new File(new File(baseIndexSwapDirectories[i], fieldTypeAndId), String.valueOf(keySize)).getAbsolutePath();
                    }
                    builder.add(keySize, new PartitionedMapChunkBackedKeyedStore(
                        new FileBackedMapChunkFactory(keySize, true, 8, false, 100, mapDirectories),
                        new FileBackedMapChunkFactory(keySize, true, 8, false, 100, swapDirectories),
                        multiChunkStore,
                        4)); //TODO expose number of partitions
                }

                indexes[fieldId] = builder.build();
            }

            MiruOnDiskFieldIndex<BM> fieldIndex = new MiruOnDiskFieldIndex<>(bitmaps, indexes);
            ((MiruOnDiskFieldIndex<BM>) from.fieldIndexProvider.getFieldIndex(fieldType)).copyTo(fieldIndex);
            fieldIndexes[fieldType.getIndex()] = fieldIndex;
        }
        MiruFieldIndexProvider<BM> fieldIndexProvider = new MiruFieldIndexProvider<>(fieldIndexes);

        LOG.info("Updated context when hybrid state changed to {}", state);
        return new MiruContext<>(schema,
            from.timeIndex,
            activityIndex,
            fieldIndexProvider,
            from.authzIndex,
            from.removalIndex,
            from.unreadTrackingIndex,
            from.inboxIndex,
            readTrackingWALReader,
            activityInternExtern,
            from.streamLocks,
            Optional.of(multiChunkStore),
            Optional.of(identifier));
    }

    @Override
    public <BM> void close(MiruContext<BM> context) {
        if (context.chunkStore.isPresent() && partitionDeleteChunkStoreOnClose) {
            try {
                context.chunkStore.get().delete();
            } catch (Exception e) {
                LOG.warn("Failed to delete chunk store", e);
            }
        }
    }

    private String[] filesToPaths(File[] files) {
        String[] paths = new String[files.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = files[i].getAbsolutePath();
        }
        return paths;
    }

}
