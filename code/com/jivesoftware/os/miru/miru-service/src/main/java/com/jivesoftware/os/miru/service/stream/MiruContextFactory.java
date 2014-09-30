package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStore;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.jive.utils.chunk.store.MultiChunkStore;
import com.jivesoftware.os.jive.utils.io.Filer;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.jive.utils.io.RandomAccessFiler;
import com.jivesoftware.os.jive.utils.keyed.store.RandomAccessSwappableFiler;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruReadTrackContext;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruFields;
import com.jivesoftware.os.miru.plugin.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruFieldIndexKey;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author jonathan
 */
public class MiruContextFactory {

    private static MetricLogger log = MetricLoggerFactory.getLogger();

    private static final String DISK_FORMAT_VERSION = "version-3";

    private final MiruSchemaProvider schemaProvider;
    private final ExecutorService executorService;
    private final ExecutorService indexingExecutorService;
    private final MiruReadTrackingWALReader readTrackingWALReader;
    private final MiruResourceLocator diskResourceLocator;
    private final MiruHybridResourceLocator hybridResourceLocator;
    private final MiruDiskResourceAnalyzer diskResourceAnalyzer = new MiruDiskResourceAnalyzer();

    private final MiruActivityInternExtern activityInternExtern;

    private final int numberOfChunkStores = 16; // TODO expose to config;
    private final int partitionAuthzCacheSize;
    private final MiruBackingStorage defaultStorage;

    public MiruContextFactory(MiruSchemaProvider schemaProvider,
        ExecutorService executorService,
        ExecutorService indexingExecutorService,
        MiruReadTrackingWALReader readTrackingWALReader,
        MiruResourceLocator diskResourceLocator,
        MiruHybridResourceLocator transientResourceLocator,
        int partitionAuthzCacheSize,
        MiruBackingStorage defaultStorage,
        MiruActivityInternExtern activityInternExtern) {
        this.schemaProvider = schemaProvider;
        this.executorService = executorService;
        this.indexingExecutorService = indexingExecutorService;
        this.readTrackingWALReader = readTrackingWALReader;
        this.diskResourceLocator = diskResourceLocator;
        this.hybridResourceLocator = transientResourceLocator;
        this.activityInternExtern = activityInternExtern;
        this.partitionAuthzCacheSize = partitionAuthzCacheSize;
        this.defaultStorage = defaultStorage;
    }

    public MiruBackingStorage findBackingStorage(MiruPartitionCoord coord) throws Exception {
        for (MiruBackingStorage storage : MiruBackingStorage.values()) {
            if (checkMarkedStorage(coord, storage, numberOfChunkStores)) {
                return storage;
            }
        }
        return defaultStorage;
    }

    public <BM> MiruContext<BM> allocate(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord, MiruBackingStorage storage) throws Exception {
        if (storage == MiruBackingStorage.memory || storage == MiruBackingStorage.memory_fixed) {
            return allocateInMemory(bitmaps, coord);
        } else if (storage == MiruBackingStorage.hybrid || storage == MiruBackingStorage.hybrid_fixed) {
            return allocateHybrid(bitmaps, coord);
        } else if (storage == MiruBackingStorage.mem_mapped) {
            return allocateMemMapped(bitmaps, coord);
        } else if (storage == MiruBackingStorage.disk) {
            return allocateOnDisk(bitmaps, coord);
        } else {
            throw new RuntimeException("backingStorage:" + storage + " is unsupported.");
        }
    }

    private <BM> MiruContext<BM> allocateInMemory(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord) {
        // check for schema first
        MiruSchema schema = schemaProvider.getSchema(coord.tenantId);

        Map<String, BulkExport<?>> exportHandles = Maps.newHashMap();

        MiruInMemoryTimeIndex timeIndex = new MiruInMemoryTimeIndex(Optional.<MiruInMemoryTimeIndex.TimeOrderAnomalyStream>absent());
        exportHandles.put("timeIndex", timeIndex);

        MiruInMemoryActivityIndex activityIndex = new MiruInMemoryActivityIndex();
        exportHandles.put("activityIndex", activityIndex);

        MiruInMemoryIndex<BM> index = new MiruInMemoryIndex<>(bitmaps);
        exportHandles.put("index", index);

        MiruInMemoryField<BM>[] fields = new MiruInMemoryField[schema.fieldCount()];
        for (int fieldId = 0; fieldId < fields.length; fieldId++) {
            fields[fieldId] = new MiruInMemoryField<>(schema.getFieldDefinition(fieldId), new ConcurrentHashMap<MiruTermId, MiruFieldIndexKey>(), index);
            exportHandles.put("field" + fieldId, fields[fieldId]);
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

        MiruIndexContext<BM> indexContext = new MiruIndexContext<>(bitmaps, schema, activityIndex, fieldIndex, authzIndex, removalIndex, activityInternExtern,
            indexingExecutorService);

        StripingLocksProvider<MiruStreamId> streamLocks = new StripingLocksProvider<>(64);
        MiruReadTrackContext<BM> readTrackContext = new MiruReadTrackContext<>(bitmaps, schema, fieldIndex, timeIndex, unreadTrackingIndex, streamLocks);

        MiruRequestContext<BM> requestContext = new MiruRequestContext<>(executorService,
            schema, timeIndex, activityIndex, fieldIndex, authzIndex, removalIndex, unreadTrackingIndex, inboxIndex,
            readTrackContext, readTrackingWALReader, streamLocks);

        return new MiruContext<>(indexContext, requestContext, readTrackContext, timeIndex, Optional.<MultiChunkStore>absent()).exportable(exportHandles);
    }

    private <BM> MiruContext<BM> allocateHybrid(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord) throws Exception {
        // check for schema first
        MiruSchema schema = schemaProvider.getSchema(coord.tenantId);

        Map<String, BulkExport<?>> exportHandles = Maps.newHashMap();

        MiruResourcePartitionIdentifier identifier = hybridResourceLocator.acquire();

        MiruInMemoryTimeIndex timeIndex = new MiruInMemoryTimeIndex(Optional.<MiruInMemoryTimeIndex.TimeOrderAnomalyStream>absent());
        exportHandles.put("timeIndex", timeIndex);

        ChunkStore[] chunkStores = new ChunkStore[numberOfChunkStores];
        for (int i = 0; i < chunkStores.length; i++) {
            File chunkFile = hybridResourceLocator.getChunkFile(identifier, "activity-" + i);
            chunkStores[i] = new ChunkStoreInitializer().initialize(chunkFile.getAbsolutePath(), hybridResourceLocator.getInitialChunkSize(), true);
        }

        MultiChunkStore multiChunkStore = new MultiChunkStore(chunkStores);

        MiruHybridActivityIndex activityIndex = new MiruHybridActivityIndex(
            hybridResourceLocator.getMapDirectory(identifier, "activity"),
            hybridResourceLocator.getSwapDirectory(identifier, "activity"),
            multiChunkStore,
            new MiruInternalActivityMarshaller(),
            Optional.<Filer>absent());
        exportHandles.put("activityIndex", activityIndex);

        MiruInMemoryIndex<BM> index = new MiruInMemoryIndex<>(bitmaps);
        exportHandles.put("index", index);

        MiruHybridField<BM>[] fields = new MiruHybridField[schema.fieldCount()];
        for (int fieldId = 0; fieldId < fields.length; fieldId++) {
            fields[fieldId] = new MiruHybridField<>(
                schema.getFieldDefinition(fieldId),
                index,
                hybridResourceLocator.getMapDirectory(identifier, "field" + fieldId));
            exportHandles.put("field" + fieldId, fields[fieldId]);
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

        MiruIndexContext<BM> indexContext = new MiruIndexContext<>(bitmaps, schema, activityIndex, fieldIndex, authzIndex, removalIndex, activityInternExtern,
            indexingExecutorService);

        StripingLocksProvider<MiruStreamId> streamLocks = new StripingLocksProvider<>(64);
        MiruReadTrackContext<BM> readTrackContext = new MiruReadTrackContext<>(bitmaps, schema, fieldIndex, timeIndex, unreadTrackingIndex, streamLocks);

        MiruRequestContext<BM> requestContext = new MiruRequestContext<>(executorService,
            schema, timeIndex, activityIndex, fieldIndex, authzIndex, removalIndex, unreadTrackingIndex, inboxIndex,
            readTrackContext, readTrackingWALReader, streamLocks);

        return new MiruContext<>(indexContext, requestContext, readTrackContext, timeIndex, Optional.of(multiChunkStore))
            .exportable(exportHandles)
            .withTransientResource(identifier);
    }

    private <BM> MiruContext<BM> allocateMemMapped(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord) throws Exception {
        //TODO refactor OnDisk impls to take a shared VariableKeySizeFileBackedKeyedStore and a prefixed KeyProvider

        // check for schema first
        MiruSchema schema = schemaProvider.getSchema(coord.tenantId);

        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);

        File versionFile = diskResourceLocator.getFilerFile(identifier, DISK_FORMAT_VERSION);
        versionFile.createNewFile();

        File memMap = diskResourceLocator.getFilerFile(identifier, "memMap");
        memMap.createNewFile();

        ChunkStore[] chunkStores = new ChunkStore[numberOfChunkStores];
        for (int i = 0; i < chunkStores.length; i++) {
            File chunkFile = diskResourceLocator.getChunkFile(identifier, "stream-" + i);
            chunkStores[i] = new ChunkStoreInitializer().initialize(chunkFile.getAbsolutePath(), diskResourceLocator.getInitialChunkSize(), true);
        }

        MultiChunkStore multiChunkStore = new MultiChunkStore(chunkStores);

        Map<String, BulkImport<?>> importHandles = Maps.newHashMap();

        MiruOnDiskTimeIndex timeIndex = new MiruOnDiskTimeIndex(
            new MemMappedFilerProvider(identifier, "timeIndex"),
            diskResourceLocator.getMapDirectory(identifier, "timestampToIndex"));
        importHandles.put("timeIndex", timeIndex);

        MiruHybridActivityIndex activityIndex = new MiruHybridActivityIndex(
            diskResourceLocator.getMapDirectory(identifier, "activity"),
            diskResourceLocator.getSwapDirectory(identifier, "activity"),
            multiChunkStore,
            new MiruInternalActivityMarshaller(),
            Optional.<Filer>of(diskResourceLocator.getByteBufferBackedFiler(identifier, "activity", 4)));

        importHandles.put("activityIndex", activityIndex);

        MiruOnDiskIndex<BM> index = new MiruOnDiskIndex<>(bitmaps,
            diskResourceLocator.getMapDirectory(identifier, "index"),
            diskResourceLocator.getSwapDirectory(identifier, "index"),
            multiChunkStore);
        importHandles.put("index", index);

        MiruOnDiskField<BM>[] fields = new MiruOnDiskField[schema.fieldCount()];
        for (int fieldId = 0; fieldId < fields.length; fieldId++) {
            fields[fieldId] = new MiruOnDiskField<>(
                schema.getFieldDefinition(fieldId),
                index,
                diskResourceLocator.getMapDirectory(identifier, "field-" + fieldId));
            importHandles.put("field" + fieldId, fields[fieldId]);
        }

        MiruFields<BM> fieldIndex = new MiruFields<>(fields, index);
        MiruAuthzUtils<BM> authzUtils = new MiruAuthzUtils<>(bitmaps);

        //TODO share the cache?
        Cache<VersionedAuthzExpression, BM> authzCache = CacheBuilder.newBuilder()
            .maximumSize(partitionAuthzCacheSize)
            .expireAfterAccess(1, TimeUnit.MINUTES) //TODO should be adjusted with respect to tuning GC (prevent promotion from eden space)
            .build();
        MiruOnDiskAuthzIndex<BM> authzIndex = new MiruOnDiskAuthzIndex<>(bitmaps,
            diskResourceLocator.getMapDirectory(identifier, "authz"),
            diskResourceLocator.getSwapDirectory(identifier, "authz"),
            multiChunkStore,
            new MiruAuthzCache<>(bitmaps, authzCache, activityInternExtern, authzUtils));
        importHandles.put("authzIndex", authzIndex);

        MiruOnDiskRemovalIndex<BM> removalIndex = new MiruOnDiskRemovalIndex<>(bitmaps, new RandomAccessSwappableFiler(
            diskResourceLocator.getFilerFile(identifier, "removal")));
        importHandles.put("removalIndex", removalIndex);

        MiruOnDiskUnreadTrackingIndex<BM> unreadTrackingIndex = new MiruOnDiskUnreadTrackingIndex<>(bitmaps,
            diskResourceLocator.getMapDirectory(identifier, "unread"),
            diskResourceLocator.getSwapDirectory(identifier, "unread"),
            multiChunkStore);
        importHandles.put("unreadTrackingIndex", unreadTrackingIndex);

        MiruOnDiskInboxIndex<BM> inboxIndex = new MiruOnDiskInboxIndex<>(bitmaps,
            diskResourceLocator.getMapDirectory(identifier, "inbox"),
            diskResourceLocator.getSwapDirectory(identifier, "inbox"),
            multiChunkStore);
        importHandles.put("inboxIndex", inboxIndex);

        MiruIndexContext<BM> indexContext = new MiruIndexContext<>(bitmaps, schema, activityIndex, fieldIndex, authzIndex, removalIndex, activityInternExtern,
            indexingExecutorService);

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

        ChunkStore[] chunkStores = new ChunkStore[numberOfChunkStores];
        for (int i = 0; i < chunkStores.length; i++) {
            File chunkFile = diskResourceLocator.getChunkFile(identifier, "stream-" + i);
            chunkStores[i] = new ChunkStoreInitializer().initialize(chunkFile.getAbsolutePath(), diskResourceLocator.getInitialChunkSize(), true);
        }

        MultiChunkStore multiChunkStore = new MultiChunkStore(chunkStores);

        MiruOnDiskTimeIndex timeIndex = new MiruOnDiskTimeIndex(
            new OnDiskFilerProvider(identifier, "timeIndex"),
            diskResourceLocator.getMapDirectory(identifier, "timestampToIndex"));
        importHandles.put("timeIndex", timeIndex);

        MiruHybridActivityIndex activityIndex = new MiruHybridActivityIndex(
            diskResourceLocator.getMapDirectory(identifier, "activity"),
            diskResourceLocator.getSwapDirectory(identifier, "activity"),
            multiChunkStore,
            new MiruInternalActivityMarshaller(),
            Optional.<Filer>of(diskResourceLocator.getRandomAccessFiler(identifier, "activity", "rw")));
        importHandles.put("activityIndex", activityIndex);

        MiruOnDiskIndex<BM> index = new MiruOnDiskIndex<>(bitmaps,
            diskResourceLocator.getMapDirectory(identifier, "index"),
            diskResourceLocator.getSwapDirectory(identifier, "index"),
            multiChunkStore);
        importHandles.put("index", index);

        MiruOnDiskField<BM>[] fields = new MiruOnDiskField[schema.fieldCount()];
        for (int fieldId = 0; fieldId < fields.length; fieldId++) {
            fields[fieldId] = new MiruOnDiskField<>(schema.getFieldDefinition(fieldId),
                index,
                diskResourceLocator.getMapDirectory(identifier, "field-" + fieldId));
            importHandles.put("field" + fieldId, fields[fieldId]);
        }

        MiruFields<BM> fieldIndex = new MiruFields<>(fields, index);
        MiruAuthzUtils<BM> authzUtils = new MiruAuthzUtils<>(bitmaps);

        Cache<VersionedAuthzExpression, BM> authzCache = CacheBuilder.newBuilder()
            .maximumSize(partitionAuthzCacheSize)
            .expireAfterAccess(1, TimeUnit.MINUTES) //TODO should be adjusted with respect to tuning GC (prevent promotion from eden space)
            .build();
        MiruOnDiskAuthzIndex<BM> authzIndex = new MiruOnDiskAuthzIndex<>(bitmaps,
            diskResourceLocator.getMapDirectory(identifier, "authz"),
            diskResourceLocator.getSwapDirectory(identifier, "authz"),
            multiChunkStore,
            new MiruAuthzCache<>(bitmaps, authzCache, activityInternExtern, authzUtils));
        importHandles.put("authzIndex", authzIndex);

        MiruOnDiskRemovalIndex<BM> removalIndex = new MiruOnDiskRemovalIndex<>(bitmaps, new RandomAccessSwappableFiler(
            diskResourceLocator.getFilerFile(identifier, "removal")));
        importHandles.put("removalIndex", removalIndex);

        MiruOnDiskUnreadTrackingIndex<BM> unreadTrackingIndex = new MiruOnDiskUnreadTrackingIndex<>(bitmaps,
            diskResourceLocator.getMapDirectory(identifier, "unread"),
            diskResourceLocator.getSwapDirectory(identifier, "unread"),
            multiChunkStore);
        importHandles.put("unreadTrackingIndex", unreadTrackingIndex);

        MiruOnDiskInboxIndex<BM> inboxIndex = new MiruOnDiskInboxIndex<>(bitmaps,
            diskResourceLocator.getMapDirectory(identifier, "inbox"),
            diskResourceLocator.getSwapDirectory(identifier, "inbox"),
            multiChunkStore);
        importHandles.put("inboxIndex", inboxIndex);

        MiruIndexContext<BM> indexContext = new MiruIndexContext<>(bitmaps, schema, activityIndex, fieldIndex, authzIndex, removalIndex, activityInternExtern,
            indexingExecutorService);

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

    @SuppressWarnings ({ "unchecked", "rawtypes" })
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
        MiruRequestContext requestContext = context.getQueryContext();
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

    private boolean checkMarkedStorage(MiruPartitionCoord coord, MiruBackingStorage storage, int numberOfChunks) throws IOException {
        File file = diskResourceLocator.getFilerFile(new MiruPartitionCoordIdentifier(coord), storage.name());
        if (file.exists()) {
            MiruSchema schema = schemaProvider.getSchema(coord.tenantId);
            if (storage == MiruBackingStorage.mem_mapped) {
                return checkMemMapped(schema, coord, numberOfChunks);
            } else if (storage == MiruBackingStorage.disk) {
                return checkOnDisk(schema, coord, numberOfChunks);
            } else {
                return true;
            }
        }
        return false;
    }

    private boolean checkMemMapped(MiruSchema schema, MiruPartitionCoord coord, int numberOfChunks) throws IOException {
        List<String> mapDirectories = Lists.newArrayList("activity", "index", "authz", "unread", "inbox", "timestampToIndex");
        for (int i = 0; i < schema.fieldCount(); i++) {
            mapDirectories.add("field-" + i);
        }
        List<String> chunkNames = new ArrayList<>();
        for (int i = 0; i < numberOfChunks; i++) {
            chunkNames.add("stream-" + i);
        }

        return diskResourceAnalyzer.checkExists(
            diskResourceLocator.getPartitionPath(new MiruPartitionCoordIdentifier(coord)),
            Lists.newArrayList(DISK_FORMAT_VERSION, "timeIndex", "activity", "removal"),
            mapDirectories,
            chunkNames);
    }

    private boolean checkOnDisk(MiruSchema schema, MiruPartitionCoord coord, int numberOfChunks) throws IOException {
        List<String> mapDirectories = Lists.newArrayList("activity", "index", "authz", "unread", "inbox", "timestampToIndex");
        for (int i = 0; i < schema.fieldCount(); i++) {
            mapDirectories.add("field-" + i);
        }
        List<String> chunkNames = new ArrayList<>();
        for (int i = 0; i < numberOfChunks; i++) {
            chunkNames.add("stream-" + i);
        }
        return diskResourceAnalyzer.checkExists(
            diskResourceLocator.getPartitionPath(new MiruPartitionCoordIdentifier(coord)),
            Lists.newArrayList(DISK_FORMAT_VERSION, "timeIndex", "activity", "removal"),
            mapDirectories,
            chunkNames);
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
