package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.RandomAccessFiler;
import com.jivesoftware.os.filer.keyed.store.PartitionedMapChunkBackedKeyedStore;
import com.jivesoftware.os.filer.keyed.store.RandomAccessSwappableFiler;
import com.jivesoftware.os.filer.keyed.store.VariableKeySizeMapChunkBackedKeyedStore;
import com.jivesoftware.os.filer.map.store.ByteBufferFactoryBackedMapChunkFactory;
import com.jivesoftware.os.filer.map.store.FileBackedMapChunkFactory;
import com.jivesoftware.os.filer.map.store.MapChunkFactory;
import com.jivesoftware.os.filer.map.store.PassThroughKeyMarshaller;
import com.jivesoftware.os.filer.map.store.VariableKeySizeBytesObjectMapStore;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruReadTrackContext;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruFields;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.plugin.schema.MiruSchemaUnvailableException;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
import com.jivesoftware.os.miru.service.index.MiruInternalActivityMarshaller;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import com.jivesoftware.os.miru.service.index.auth.VersionedAuthzExpression;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskAuthzIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskField;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskInboxIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskRemovalIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskTimeIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruHybridActivityIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruHybridField;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryActivityIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryAuthzIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryField;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryInboxIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryRemovalIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryTimeIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.locator.MiruDiskResourceAnalyzer;
import com.jivesoftware.os.miru.service.locator.MiruHybridResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruPartitionCoordIdentifier;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruResourcePartitionIdentifier;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author jonathan
 */
public class MiruContextFactory {

    private static MetricLogger log = MetricLoggerFactory.getLogger();

    private static final String DISK_FORMAT_VERSION = "version-6";

    private final MiruSchemaProvider schemaProvider;
    private final ExecutorService executorService;
    private final MiruReadTrackingWALReader readTrackingWALReader;
    private final MiruResourceLocator diskResourceLocator;
    private final MiruHybridResourceLocator hybridResourceLocator;
    private final MiruDiskResourceAnalyzer diskResourceAnalyzer = new MiruDiskResourceAnalyzer();

    private final MiruActivityInternExtern activityInternExtern;

    private final int numberOfChunkStores = 24; // TODO expose to config;
    private final int partitionAuthzCacheSize;
    private final MiruBackingStorage defaultStorage;

    public MiruContextFactory(MiruSchemaProvider schemaProvider,
        ExecutorService executorService,
        MiruReadTrackingWALReader readTrackingWALReader,
        MiruResourceLocator diskResourceLocator,
        MiruHybridResourceLocator transientResourceLocator,
        int partitionAuthzCacheSize,
        MiruBackingStorage defaultStorage,
        MiruActivityInternExtern activityInternExtern) {
        this.schemaProvider = schemaProvider;
        this.executorService = executorService;
        this.readTrackingWALReader = readTrackingWALReader;
        this.diskResourceLocator = diskResourceLocator;
        this.hybridResourceLocator = transientResourceLocator;
        this.activityInternExtern = activityInternExtern;
        this.partitionAuthzCacheSize = partitionAuthzCacheSize;
        this.defaultStorage = defaultStorage;
    }

    public MiruBackingStorage findBackingStorage(MiruPartitionCoord coord) throws Exception {
        try {
            for (MiruBackingStorage storage : MiruBackingStorage.values()) {
                if (checkMarkedStorage(coord, storage, numberOfChunkStores)) {
                    return storage;
                }
            }
        } catch (MiruSchemaUnvailableException e) {
            log.warn("Schema not registered for tenant {}, using default storage", coord.tenantId);
        }
        return defaultStorage;
    }

    public <BM> MiruContext<BM> allocate(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord, MiruBackingStorage storage, ByteBufferFactory byteBufferFactory)
        throws Exception {
        if (storage == MiruBackingStorage.memory || storage == MiruBackingStorage.memory_fixed) {
            return allocateInMemory(bitmaps, coord, byteBufferFactory);
        } else if (storage == MiruBackingStorage.hybrid || storage == MiruBackingStorage.hybrid_fixed) {
            return allocateHybrid(bitmaps, coord, byteBufferFactory);
        } else if (storage == MiruBackingStorage.mem_mapped) {
            return allocateMemMapped(bitmaps, coord);
        } else if (storage == MiruBackingStorage.disk) {
            return allocateOnDisk(bitmaps, coord);
        } else {
            throw new RuntimeException("backingStorage:" + storage + " is unsupported.");
        }
    }

    //TODO push to schema
    private static final int[] IN_MEMORY_FIELD_KEY_SIZE_THRESHOLDS = new int[] { 2, 4, 6, 8, 10, 12, 16, 64, 256, 1_024 };
    //private static final int[] IN_MEMORY_FIELD_KEY_SIZE_PARTITIONS = new int[] { 1, 2, 2, 6, 8, 6, 2, 1, 1, 1 };

    private <BM> MiruContext<BM> allocateInMemory(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord, ByteBufferFactory byteBufferFactory) throws Exception {
        // check for schema first
        MiruSchema schema = schemaProvider.getSchema(coord.tenantId);

        Map<String, BulkExport<?>> exportHandles = Maps.newHashMap();

        MiruInMemoryTimeIndex timeIndex = new MiruInMemoryTimeIndex(Optional.<MiruInMemoryTimeIndex.TimeOrderAnomalyStream>absent(), byteBufferFactory);
        exportHandles.put("timeIndex", timeIndex);

        MiruInMemoryActivityIndex activityIndex = new MiruInMemoryActivityIndex();
        exportHandles.put("activityIndex", activityIndex);

        @SuppressWarnings("unchecked")
        VariableKeySizeBytesObjectMapStore<byte[], MiruInvertedIndex<BM>>[] indexes = new VariableKeySizeBytesObjectMapStore[schema.fieldCount()];
        for (int fieldId : schema.getFieldIds()) {
            indexes[fieldId] = new VariableKeySizeBytesObjectMapStore<>(IN_MEMORY_FIELD_KEY_SIZE_THRESHOLDS, 10, null, byteBufferFactory,
                PassThroughKeyMarshaller.INSTANCE);
        }

        MiruInMemoryIndex<BM> index = new MiruInMemoryIndex<>(bitmaps, indexes);
        exportHandles.put("index", index);

        @SuppressWarnings("unchecked")
        MiruInMemoryField<BM>[] fields = new MiruInMemoryField[schema.fieldCount()];
        for (int fieldId = 0; fieldId < fields.length; fieldId++) {
            fields[fieldId] = new MiruInMemoryField<>(schema.getFieldDefinition(fieldId), index);
        }

        MiruFields<BM> fieldIndex = new MiruFields<>(fields, index);
        MiruAuthzUtils<BM> authzUtils = new MiruAuthzUtils<>(bitmaps);

        //TODO share the cache?
        Cache<VersionedAuthzExpression, BM> authzCache = CacheBuilder.newBuilder()
            .maximumSize(partitionAuthzCacheSize)
            .expireAfterAccess(1, TimeUnit.MINUTES) //TODO should be adjusted with respect to tuning GC (prevent promotion from eden space)
            .build();
        MiruInMemoryAuthzIndex<BM> authzIndex = new MiruInMemoryAuthzIndex<>(
            bitmaps, new MiruAuthzCache<>(bitmaps, authzCache, activityInternExtern, authzUtils));
        exportHandles.put("authzIndex", authzIndex);

        MiruInMemoryRemovalIndex<BM> removalIndex = new MiruInMemoryRemovalIndex<>(bitmaps);
        exportHandles.put("removalIndex", removalIndex);

        MiruInMemoryUnreadTrackingIndex<BM> unreadTrackingIndex = new MiruInMemoryUnreadTrackingIndex<>(bitmaps);
        exportHandles.put("unreadTrackingIndex", unreadTrackingIndex);

        MiruInMemoryInboxIndex<BM> inboxIndex = new MiruInMemoryInboxIndex<>(bitmaps);
        exportHandles.put("inboxIndex", inboxIndex);

        MiruIndexContext<BM> indexContext = new MiruIndexContext<>(bitmaps, schema, activityIndex, fieldIndex, authzIndex, removalIndex, activityInternExtern);

        StripingLocksProvider<MiruStreamId> streamLocks = new StripingLocksProvider<>(64);
        MiruReadTrackContext<BM> readTrackContext = new MiruReadTrackContext<>(bitmaps, schema, fieldIndex, timeIndex, unreadTrackingIndex, streamLocks);

        MiruRequestContext<BM> requestContext = new MiruRequestContext<>(executorService,
            schema, timeIndex, activityIndex, fieldIndex, authzIndex, removalIndex, unreadTrackingIndex, inboxIndex,
            readTrackContext, readTrackingWALReader, streamLocks);

        return new MiruContext<>(indexContext, requestContext, readTrackContext, timeIndex, Optional.<MultiChunkStore>absent()).exportable(exportHandles);
    }

    private <BM> MiruContext<BM> allocateHybrid(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord, ByteBufferFactory byteBufferFactory) throws Exception {
        // check for schema first
        MiruSchema schema = schemaProvider.getSchema(coord.tenantId);

        Map<String, BulkExport<?>> exportHandles = Maps.newHashMap();

        MiruResourcePartitionIdentifier identifier = hybridResourceLocator.acquire();

        MiruInMemoryTimeIndex timeIndex = new MiruInMemoryTimeIndex(Optional.<MiruInMemoryTimeIndex.TimeOrderAnomalyStream>absent(), byteBufferFactory);
        exportHandles.put("timeIndex", timeIndex);

        MultiChunkStore multiChunkStore;
        if (hybridResourceLocator.isFileBackedChunkStore()) {
            multiChunkStore = new ChunkStoreInitializer().initializeMulti(
                filesToPaths(hybridResourceLocator.getChunkDirectories(identifier, "chunk")),
                "activity",
                numberOfChunkStores,
                hybridResourceLocator.getInitialChunkSize(),
                true);
        } else {
            ChunkStoreInitializer initializer = new ChunkStoreInitializer();
            ChunkStoreInitializer.MultiChunkStoreBuilder builder = new ChunkStoreInitializer.MultiChunkStoreBuilder();
            for (int i = 0; i < numberOfChunkStores; i++) {
                builder.addChunkStore(initializer.create(new Object(), byteBufferFactory, hybridResourceLocator.getInitialChunkSize(), true));
            }
            multiChunkStore = builder.build();
        }

        MapChunkFactory mapChunkFactory = new ByteBufferFactoryBackedMapChunkFactory(4, false, 8, false, 100, byteBufferFactory);
        MiruHybridActivityIndex activityIndex = new MiruHybridActivityIndex(
            mapChunkFactory,
            mapChunkFactory,
            multiChunkStore,
            new MiruInternalActivityMarshaller(),
            Optional.<Filer>absent());
        exportHandles.put("activityIndex", activityIndex);

        @SuppressWarnings("unchecked")
        VariableKeySizeBytesObjectMapStore<byte[], MiruInvertedIndex<BM>>[] indexes = new VariableKeySizeBytesObjectMapStore[schema.fieldCount()];
        for (int fieldId : schema.getFieldIds()) {
            indexes[fieldId] = new VariableKeySizeBytesObjectMapStore<>(IN_MEMORY_FIELD_KEY_SIZE_THRESHOLDS, 10, null, byteBufferFactory,
                PassThroughKeyMarshaller.INSTANCE);
        }

        MiruInMemoryIndex<BM> index = new MiruInMemoryIndex<>(bitmaps, indexes);
        exportHandles.put("index", index);

        @SuppressWarnings("unchecked")
        MiruHybridField<BM>[] fields = new MiruHybridField[schema.fieldCount()];
        for (int fieldId = 0; fieldId < fields.length; fieldId++) {
            fields[fieldId] = new MiruHybridField<>(schema.getFieldDefinition(fieldId), index);
        }

        MiruFields<BM> fieldIndex = new MiruFields<>(fields, index);
        MiruAuthzUtils<BM> authzUtils = new MiruAuthzUtils<>(bitmaps);

        //TODO share the cache?
        Cache<VersionedAuthzExpression, BM> authzCache = CacheBuilder.newBuilder()
            .maximumSize(partitionAuthzCacheSize)
            .expireAfterAccess(1, TimeUnit.MINUTES) //TODO should be adjusted with respect to tuning GC (prevent promotion from eden space)
            .build();
        MiruInMemoryAuthzIndex<BM> authzIndex = new MiruInMemoryAuthzIndex<>(bitmaps,
            new MiruAuthzCache<>(bitmaps, authzCache, activityInternExtern, authzUtils));
        exportHandles.put("authzIndex", authzIndex);

        MiruInMemoryRemovalIndex<BM> removalIndex = new MiruInMemoryRemovalIndex<>(bitmaps);
        exportHandles.put("removalIndex", removalIndex);

        MiruInMemoryUnreadTrackingIndex<BM> unreadTrackingIndex = new MiruInMemoryUnreadTrackingIndex<>(bitmaps);
        exportHandles.put("unreadTrackingIndex", unreadTrackingIndex);

        MiruInMemoryInboxIndex<BM> inboxIndex = new MiruInMemoryInboxIndex<>(bitmaps);
        exportHandles.put("inboxIndex", inboxIndex);

        MiruIndexContext<BM> indexContext = new MiruIndexContext<>(bitmaps, schema, activityIndex, fieldIndex, authzIndex, removalIndex, activityInternExtern);

        StripingLocksProvider<MiruStreamId> streamLocks = new StripingLocksProvider<>(64);
        MiruReadTrackContext<BM> readTrackContext = new MiruReadTrackContext<>(bitmaps, schema, fieldIndex, timeIndex, unreadTrackingIndex, streamLocks);

        MiruRequestContext<BM> requestContext = new MiruRequestContext<>(executorService,
            schema, timeIndex, activityIndex, fieldIndex, authzIndex, removalIndex, unreadTrackingIndex, inboxIndex,
            readTrackContext, readTrackingWALReader, streamLocks);

        return new MiruContext<>(indexContext, requestContext, readTrackContext, timeIndex, Optional.of(multiChunkStore))
            .exportable(exportHandles)
            .withTransientResource(identifier);
    }

    //TODO push to schema
    private static final int[] ON_DISK_FIELD_KEY_SIZE_THRESHOLDS = new int[] { 4, 16, 64, 256, 1_024 };

    private <BM> MiruContext<BM> allocateMemMapped(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord) throws Exception {
        //TODO refactor OnDisk impls to take a shared VariableKeySizeFileBackedKeyedStore and a prefixed KeyProvider

        // check for schema first
        MiruSchema schema = schemaProvider.getSchema(coord.tenantId);

        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);

        File versionFile = diskResourceLocator.getFilerFile(identifier, DISK_FORMAT_VERSION);
        versionFile.createNewFile();

        File memMap = diskResourceLocator.getFilerFile(identifier, "memMap");
        memMap.createNewFile();

        MultiChunkStore multiChunkStore = new ChunkStoreInitializer().initializeMulti(
            filesToPaths(diskResourceLocator.getChunkDirectories(identifier, "chunk")),
            "stream",
            numberOfChunkStores,
            diskResourceLocator.getInitialChunkSize(),
            true);

        Map<String, BulkImport<?>> importHandles = Maps.newHashMap();

        MiruOnDiskTimeIndex timeIndex = new MiruOnDiskTimeIndex(
            new MemMappedFilerProvider(identifier, "timeIndex"),
            filesToPaths(diskResourceLocator.getMapDirectories(identifier, "timestampToIndex")));
        importHandles.put("timeIndex", timeIndex);

        MiruHybridActivityIndex activityIndex = new MiruHybridActivityIndex(
            new FileBackedMapChunkFactory(4, false, 8, false, 100,
                filesToPaths(diskResourceLocator.getMapDirectories(identifier, "activity"))),
            new FileBackedMapChunkFactory(4, false, 8, false, 100,
                filesToPaths(diskResourceLocator.getSwapDirectories(identifier, "activity"))),
            multiChunkStore,
            new MiruInternalActivityMarshaller(),
            Optional.<Filer>of(diskResourceLocator.getRandomAccessFiler(identifier, "activity", "rw")));
        importHandles.put("activityIndex", activityIndex);

        File[] baseIndexMapDirectories = diskResourceLocator.getMapDirectories(identifier, "index");
        File[] baseIndexSwapDirectories = diskResourceLocator.getSwapDirectories(identifier, "index");

        VariableKeySizeMapChunkBackedKeyedStore[] indexes = new VariableKeySizeMapChunkBackedKeyedStore[schema.fieldCount()];
        for (int fieldId : schema.getFieldIds()) {
            //TODO expose to config
            VariableKeySizeMapChunkBackedKeyedStore.Builder builder = new VariableKeySizeMapChunkBackedKeyedStore.Builder();

            for (int keySize : ON_DISK_FIELD_KEY_SIZE_THRESHOLDS) {
                String[] mapDirectories = new String[baseIndexMapDirectories.length];
                for (int i = 0; i < mapDirectories.length; i++) {
                    mapDirectories[i] = new File(new File(baseIndexMapDirectories[i], String.valueOf(fieldId)), String.valueOf(keySize)).getAbsolutePath();
                }
                String[] swapDirectories = new String[baseIndexSwapDirectories.length];
                for (int i = 0; i < swapDirectories.length; i++) {
                    swapDirectories[i] = new File(new File(baseIndexSwapDirectories[i], String.valueOf(fieldId)), String.valueOf(keySize)).getAbsolutePath();
                }
                builder.add(keySize, new PartitionedMapChunkBackedKeyedStore(
                    new FileBackedMapChunkFactory(keySize, true, 8, false, 100, mapDirectories),
                    new FileBackedMapChunkFactory(keySize, true, 8, false, 100, swapDirectories),
                    multiChunkStore,
                    4)); //TODO expose number of partitions
            }

            indexes[fieldId] = builder.build();
        }

        MiruOnDiskIndex<BM> index = new MiruOnDiskIndex<>(bitmaps, indexes);
        importHandles.put("index", index);

        @SuppressWarnings("unchecked")
        MiruOnDiskField<BM>[] fields = new MiruOnDiskField[schema.fieldCount()];
        for (int fieldId = 0; fieldId < fields.length; fieldId++) {
            fields[fieldId] = new MiruOnDiskField<>(
                schema.getFieldDefinition(fieldId),
                index);
        }

        MiruFields<BM> fieldIndex = new MiruFields<>(fields, index);
        MiruAuthzUtils<BM> authzUtils = new MiruAuthzUtils<>(bitmaps);

        //TODO share the cache?
        Cache<VersionedAuthzExpression, BM> authzCache = CacheBuilder.newBuilder()
            .maximumSize(partitionAuthzCacheSize)
            .expireAfterAccess(1, TimeUnit.MINUTES) //TODO should be adjusted with respect to tuning GC (prevent promotion from eden space)
            .build();
        MiruOnDiskAuthzIndex<BM> authzIndex = new MiruOnDiskAuthzIndex<>(bitmaps,
            filesToPaths(diskResourceLocator.getMapDirectories(identifier, "authz")),
            filesToPaths(diskResourceLocator.getSwapDirectories(identifier, "authz")),
            multiChunkStore,
            new MiruAuthzCache<>(bitmaps, authzCache, activityInternExtern, authzUtils));
        importHandles.put("authzIndex", authzIndex);

        MiruOnDiskRemovalIndex<BM> removalIndex = new MiruOnDiskRemovalIndex<>(bitmaps, new RandomAccessSwappableFiler(
            diskResourceLocator.getFilerFile(identifier, "removal")));
        importHandles.put("removalIndex", removalIndex);

        MiruOnDiskUnreadTrackingIndex<BM> unreadTrackingIndex = new MiruOnDiskUnreadTrackingIndex<>(bitmaps,
            filesToPaths(diskResourceLocator.getMapDirectories(identifier, "unread")),
            filesToPaths(diskResourceLocator.getSwapDirectories(identifier, "unread")),
            multiChunkStore);
        importHandles.put("unreadTrackingIndex", unreadTrackingIndex);

        MiruOnDiskInboxIndex<BM> inboxIndex = new MiruOnDiskInboxIndex<>(bitmaps,
            filesToPaths(diskResourceLocator.getMapDirectories(identifier, "inbox")),
            filesToPaths(diskResourceLocator.getSwapDirectories(identifier, "inbox")),
            multiChunkStore);
        importHandles.put("inboxIndex", inboxIndex);

        MiruIndexContext<BM> indexContext = new MiruIndexContext<>(bitmaps, schema, activityIndex, fieldIndex, authzIndex, removalIndex, activityInternExtern);

        StripingLocksProvider<MiruStreamId> streamLocks = new StripingLocksProvider<>(64);
        MiruReadTrackContext<BM> readTrackContext = new MiruReadTrackContext<>(bitmaps, schema, fieldIndex, timeIndex, unreadTrackingIndex, streamLocks);

        MiruRequestContext<BM> requestContext = new MiruRequestContext<>(executorService,
            schema, timeIndex, activityIndex, fieldIndex, authzIndex, removalIndex, unreadTrackingIndex, inboxIndex,
            readTrackContext, readTrackingWALReader, streamLocks);

        return new MiruContext<>(indexContext, requestContext, readTrackContext, timeIndex, Optional.of(multiChunkStore)).importable(importHandles);
    }

    private <BM> MiruContext<BM> allocateOnDisk(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord) throws Exception {
        //TODO refactor OnDisk impls to take a shared VariableKeySizeFileBackedKeyedStore and a prefixed KeyProvider

        // check for schema first
        MiruSchema schema = schemaProvider.getSchema(coord.tenantId);

        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        Map<String, BulkImport<?>> importHandles = Maps.newHashMap();

        File versionFile = diskResourceLocator.getFilerFile(identifier, DISK_FORMAT_VERSION);
        versionFile.createNewFile();

        File onDisk = diskResourceLocator.getFilerFile(identifier, "onDisk");
        onDisk.createNewFile();

        MultiChunkStore multiChunkStore = new ChunkStoreInitializer().initializeMulti(
            filesToPaths(diskResourceLocator.getChunkDirectories(identifier, "chunk")),
            "stream",
            numberOfChunkStores,
            diskResourceLocator.getInitialChunkSize(),
            true);

        MiruOnDiskTimeIndex timeIndex = new MiruOnDiskTimeIndex(
            new OnDiskFilerProvider(identifier, "timeIndex"),
            filesToPaths(diskResourceLocator.getMapDirectories(identifier, "timestampToIndex")));
        importHandles.put("timeIndex", timeIndex);

        MiruHybridActivityIndex activityIndex = new MiruHybridActivityIndex(
            new FileBackedMapChunkFactory(4, false, 8, false, 100,
                filesToPaths(diskResourceLocator.getMapDirectories(identifier, "activity"))),
            new FileBackedMapChunkFactory(4, false, 8, false, 100,
                filesToPaths(diskResourceLocator.getSwapDirectories(identifier, "activity"))),
            multiChunkStore,
            new MiruInternalActivityMarshaller(),
            Optional.<Filer>of(diskResourceLocator.getRandomAccessFiler(identifier, "activity", "rw")));
        importHandles.put("activityIndex", activityIndex);

        File[] baseIndexMapDirectories = diskResourceLocator.getMapDirectories(identifier, "index");
        File[] baseIndexSwapDirectories = diskResourceLocator.getSwapDirectories(identifier, "index");

        VariableKeySizeMapChunkBackedKeyedStore[] indexes = new VariableKeySizeMapChunkBackedKeyedStore[schema.fieldCount()];
        for (int fieldId : schema.getFieldIds()) {
            //TODO expose to config
            VariableKeySizeMapChunkBackedKeyedStore.Builder builder = new VariableKeySizeMapChunkBackedKeyedStore.Builder();

            for (int keySize : ON_DISK_FIELD_KEY_SIZE_THRESHOLDS) {
                String[] mapDirectories = new String[baseIndexMapDirectories.length];
                for (int i = 0; i < mapDirectories.length; i++) {
                    mapDirectories[i] = new File(new File(baseIndexMapDirectories[i], String.valueOf(fieldId)), String.valueOf(keySize)).getAbsolutePath();
                }
                String[] swapDirectories = new String[baseIndexSwapDirectories.length];
                for (int i = 0; i < swapDirectories.length; i++) {
                    swapDirectories[i] = new File(new File(baseIndexSwapDirectories[i], String.valueOf(fieldId)), String.valueOf(keySize)).getAbsolutePath();
                }
                builder.add(keySize, new PartitionedMapChunkBackedKeyedStore(
                    new FileBackedMapChunkFactory(keySize, true, 8, false, 100, mapDirectories),
                    new FileBackedMapChunkFactory(keySize, true, 8, false, 100, swapDirectories),
                    multiChunkStore,
                    4)); //TODO expose number of partitions
            }

            indexes[fieldId] = builder.build();
        }

        MiruOnDiskIndex<BM> index = new MiruOnDiskIndex<>(bitmaps, indexes);
        importHandles.put("index", index);

        @SuppressWarnings("unchecked")
        MiruOnDiskField<BM>[] fields = new MiruOnDiskField[schema.fieldCount()];
        for (int fieldId = 0; fieldId < fields.length; fieldId++) {
            fields[fieldId] = new MiruOnDiskField<>(schema.getFieldDefinition(fieldId),
                index);
        }

        MiruFields<BM> fieldIndex = new MiruFields<>(fields, index);
        MiruAuthzUtils<BM> authzUtils = new MiruAuthzUtils<>(bitmaps);

        Cache<VersionedAuthzExpression, BM> authzCache = CacheBuilder.newBuilder()
            .maximumSize(partitionAuthzCacheSize)
            .expireAfterAccess(1, TimeUnit.MINUTES) //TODO should be adjusted with respect to tuning GC (prevent promotion from eden space)
            .build();
        MiruOnDiskAuthzIndex<BM> authzIndex = new MiruOnDiskAuthzIndex<>(bitmaps,
            filesToPaths(diskResourceLocator.getMapDirectories(identifier, "authz")),
            filesToPaths(diskResourceLocator.getSwapDirectories(identifier, "authz")),
            multiChunkStore,
            new MiruAuthzCache<>(bitmaps, authzCache, activityInternExtern, authzUtils));
        importHandles.put("authzIndex", authzIndex);

        MiruOnDiskRemovalIndex<BM> removalIndex = new MiruOnDiskRemovalIndex<>(bitmaps, new RandomAccessSwappableFiler(
            diskResourceLocator.getFilerFile(identifier, "removal")));
        importHandles.put("removalIndex", removalIndex);

        MiruOnDiskUnreadTrackingIndex<BM> unreadTrackingIndex = new MiruOnDiskUnreadTrackingIndex<>(bitmaps,
            filesToPaths(diskResourceLocator.getMapDirectories(identifier, "unread")),
            filesToPaths(diskResourceLocator.getSwapDirectories(identifier, "unread")),
            multiChunkStore);
        importHandles.put("unreadTrackingIndex", unreadTrackingIndex);

        MiruOnDiskInboxIndex<BM> inboxIndex = new MiruOnDiskInboxIndex<>(bitmaps,
            filesToPaths(diskResourceLocator.getMapDirectories(identifier, "inbox")),
            filesToPaths(diskResourceLocator.getSwapDirectories(identifier, "inbox")),
            multiChunkStore);
        importHandles.put("inboxIndex", inboxIndex);

        MiruIndexContext<BM> indexContext = new MiruIndexContext<>(bitmaps, schema, activityIndex, fieldIndex, authzIndex, removalIndex, activityInternExtern);

        StripingLocksProvider<MiruStreamId> streamLocks = new StripingLocksProvider<>(64);
        MiruReadTrackContext<BM> readTrackContext = new MiruReadTrackContext<>(bitmaps, schema, fieldIndex, timeIndex, unreadTrackingIndex, streamLocks);

        MiruRequestContext<BM> requestContext = new MiruRequestContext<>(executorService,
            schema, timeIndex, activityIndex, fieldIndex, authzIndex, removalIndex, unreadTrackingIndex, inboxIndex,
            readTrackContext, readTrackingWALReader, streamLocks);

        return new MiruContext<>(indexContext, requestContext, readTrackContext, timeIndex, Optional.of(multiChunkStore)).importable(importHandles);
    }

    public <BM> MiruContext<BM> copyMemMapped(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord, MiruContext<BM> from) throws Exception {
        return copy(coord.tenantId, from, allocateMemMapped(bitmaps, coord));
    }

    public <BM> MiruContext<BM> copyToDisk(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord, MiruContext<BM> from) throws Exception {
        return copy(coord.tenantId, from, allocateOnDisk(bitmaps, coord));
    }

    public void markStorage(MiruPartitionCoord coord, MiruBackingStorage marked) throws Exception {
        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);

        for (MiruBackingStorage storage : MiruBackingStorage.values()) {
            if (storage != marked) {
                diskResourceLocator.getFilerFile(identifier, storage.name()).delete();
            }
        }

        diskResourceLocator.getFilerFile(identifier, marked.name()).createNewFile();
    }

    public void markSip(MiruPartitionCoord coord, long sipTimestamp) throws Exception {
        File file = diskResourceLocator.getFilerFile(new MiruPartitionCoordIdentifier(coord), "sip");
        try (Filer filer = new RandomAccessFiler(file, "rw")) {
            filer.setLength(0);
            filer.seek(0);
            FilerIO.writeLong(filer, sipTimestamp, "sip");
        }
    }

    public long getSip(MiruPartitionCoord coord) throws Exception {
        File file = diskResourceLocator.getFilerFile(new MiruPartitionCoordIdentifier(coord), "sip");
        if (file.exists()) {
            try (Filer filer = new RandomAccessFiler(file, "rw")) {
                filer.seek(0);
                return FilerIO.readLong(filer, "sip");
            }
        } else {
            return 0;
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private <BM> MiruContext<BM> copy(MiruTenantId tenantId, MiruContext<BM> from, MiruContext<BM> to) throws Exception {
        Map<String, BulkImport<?>> importHandles = to.getImportHandles();
        for (Map.Entry<String, BulkExport<?>> entry : from.getExportHandles().entrySet()) {
            String key = entry.getKey();
            BulkExport<?> bulkExport = entry.getValue();
            BulkImport<?> bulkImport = importHandles.get(key);
            if (bulkImport != null) {
                bulkImport.bulkImport(tenantId, (BulkExport) bulkExport);
            } else {
                log.warn("Missing bulk importer for {}", key);
            }
        }
        return to;
    }

    public void cleanDisk(MiruPartitionCoord coord) throws IOException {
        diskResourceLocator.clean(new MiruPartitionCoordIdentifier(coord));
    }

    public <BM> void close(MiruContext<BM> context) {
        MiruRequestContext requestContext = context.getRequestContext();
        requestContext.activityIndex.close();
        requestContext.authzIndex.close();
        requestContext.timeIndex.close();
        requestContext.unreadTrackingIndex.close();
        requestContext.inboxIndex.close();

        Optional<? extends MiruResourcePartitionIdentifier> transientResource = context.getTransientResource();
        if (transientResource.isPresent()) {
            hybridResourceLocator.release(transientResource.get());
        }
    }

    private boolean checkMarkedStorage(MiruPartitionCoord coord, MiruBackingStorage storage, int numberOfChunks) throws Exception {
        File file = diskResourceLocator.getFilerFile(new MiruPartitionCoordIdentifier(coord), storage.name());
        if (file.exists()) {
            if (storage == MiruBackingStorage.mem_mapped) {
                return checkMemMapped(coord, numberOfChunks);
            } else if (storage == MiruBackingStorage.disk) {
                return checkOnDisk(coord, numberOfChunks);
            } else {
                return true;
            }
        }
        return false;
    }

    private boolean checkMemMapped(MiruPartitionCoord coord, int numberOfChunks) throws IOException {
        List<String> mapDirectories = Lists.newArrayList("activity", "index", "authz", "unread", "inbox", "timestampToIndex");

        MiruPartitionCoordIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        String[] chunkPaths = filesToPaths(diskResourceLocator.getChunkDirectories(identifier, "chunk"));
        if (!new ChunkStoreInitializer().checkExists(chunkPaths, "stream", numberOfChunks)) {
            return false;
        }

        return diskResourceAnalyzer.checkExists(
            diskResourceLocator.getPartitionPaths(identifier),
            Lists.newArrayList(DISK_FORMAT_VERSION, "timeIndex", "activity", "removal"),
            mapDirectories);
    }

    private boolean checkOnDisk(MiruPartitionCoord coord, int numberOfChunks) throws IOException {
        List<String> mapDirectories = Lists.newArrayList("activity", "index", "authz", "unread", "inbox", "timestampToIndex");

        MiruPartitionCoordIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        String[] chunkPaths = filesToPaths(diskResourceLocator.getChunkDirectories(identifier, "chunk"));
        if (!new ChunkStoreInitializer().checkExists(chunkPaths, "stream", numberOfChunks)) {
            return false;
        }

        return diskResourceAnalyzer.checkExists(
            diskResourceLocator.getPartitionPaths(identifier),
            Lists.newArrayList(DISK_FORMAT_VERSION, "timeIndex", "activity", "removal"),
            mapDirectories);
    }

    private String[] filesToPaths(File[] files) {
        String[] paths = new String[files.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = files[i].getAbsolutePath();
        }
        return paths;
    }

    private class MemMappedFilerProvider implements MiruFilerProvider {

        private final MiruResourcePartitionIdentifier identifier;
        private final String name;

        private MemMappedFilerProvider(MiruResourcePartitionIdentifier identifier, String name) {
            this.identifier = identifier;
            this.name = name;
        }

        @Override
        public File getBackingFile() throws IOException {
            return diskResourceLocator.getFilerFile(identifier, name);
        }

        @Override
        public Filer getFiler(long length) throws IOException {
            return diskResourceLocator.getByteBufferBackedFiler(identifier, name, length);
        }
    }

    private class OnDiskFilerProvider implements MiruFilerProvider {

        private final MiruResourcePartitionIdentifier identifier;
        private final String name;

        private OnDiskFilerProvider(MiruResourcePartitionIdentifier identifier, String name) {
            this.identifier = identifier;
            this.name = name;
        }

        @Override
        public File getBackingFile() throws IOException {
            return diskResourceLocator.getFilerFile(identifier, name);
        }

        @Override
        public Filer getFiler(long length) throws IOException {
            return diskResourceLocator.getRandomAccessFiler(identifier, name, "rw");
        }
    }

}
