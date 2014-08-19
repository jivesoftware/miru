package com.jivesoftware.os.miru.service.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStore;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.jive.utils.io.Filer;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.jive.utils.io.RandomAccessFiler;
import com.jivesoftware.os.jive.utils.keyed.store.RandomAccessSwappableFiler;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.service.index.*;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import com.jivesoftware.os.miru.service.index.auth.VersionedAuthzExpression;
import com.jivesoftware.os.miru.service.index.disk.*;
import com.jivesoftware.os.miru.service.index.memory.*;
import com.jivesoftware.os.miru.service.schema.MiruSchema;
import com.jivesoftware.os.miru.service.stream.factory.MiruFilterUtils;
import com.jivesoftware.os.miru.service.stream.locator.*;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;

import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author jonathan
 */
@Singleton
public class MiruStreamFactory {

    private static MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruSchema schema;
    private final ExecutorService executorService;
    private final MiruReadTrackingWALReader readTrackingWALReader;
    private final MiruResourceLocator diskResourceLocator;
    private final MiruTransientResourceLocator transientResourceLocator;
    private final MiruDiskResourceAnalyzer diskResourceAnalyzer = new MiruDiskResourceAnalyzer();

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Interner<String> stringInterner = Interners.newWeakInterner();
    private final MiruActivityInterner activityInterner = new MiruActivityInterner(
            Interners.<MiruIBA>newWeakInterner(),
            Interners.<MiruTermId>newWeakInterner(),
            // tenants are strongly interned
            Interners.<MiruTenantId>newStrongInterner(),
            // makes sense to share string internment as this is authz in both cases
            stringInterner);

    private final int bitsetBufferSize;
    private final int partitionAuthzCacheSize;
    private final MiruBackingStorage defaultStorage;

    public MiruStreamFactory(MiruSchema schema,
            ExecutorService executorService,
            MiruReadTrackingWALReader readTrackingWALReader,
            MiruResourceLocator diskResourceLocator,
            MiruTransientResourceLocator transientResourceLocator,
            int bitsetBufferSize,
            int partitionAuthzCacheSize,
            MiruBackingStorage defaultStorage) {
        this.schema = schema;
        this.executorService = executorService;
        this.readTrackingWALReader = readTrackingWALReader;
        this.diskResourceLocator = diskResourceLocator;
        this.transientResourceLocator = transientResourceLocator;
        this.bitsetBufferSize = bitsetBufferSize;
        this.partitionAuthzCacheSize = partitionAuthzCacheSize;
        this.defaultStorage = defaultStorage;
    }

    public MiruBackingStorage findBackingStorage(MiruPartitionCoord coord) throws Exception {
        for (MiruBackingStorage storage : MiruBackingStorage.values()) {
            if (checkMarkedStorage(coord, storage)) {
                return storage;
            }
        }
        return defaultStorage;
    }

    public MiruStream allocate(MiruPartitionCoord coord, MiruBackingStorage storage) throws Exception {
        if (storage == MiruBackingStorage.memory || storage == MiruBackingStorage.memory_fixed) {
            return allocateInMemory();
        } else if (storage == MiruBackingStorage.hybrid || storage == MiruBackingStorage.hybrid_fixed) {
            return allocateTransient();
        } else if (storage == MiruBackingStorage.mem_mapped) {
            return allocateMemMapped(coord);
        } else if (storage == MiruBackingStorage.disk) {
            return allocateOnDisk(coord);
        } else {
            throw new RuntimeException("backingStorage:" + storage + " is unsupported.");
        }
    }

    private MiruStream allocateInMemory() {
        Map<String, BulkExport<?>> exportHandles = Maps.newHashMap();

        MiruInMemoryTimeIndex timeIndex = new MiruInMemoryTimeIndex(Optional.<MiruInMemoryTimeIndex.TimeOrderAnomalyStream>absent());
        exportHandles.put("timeIndex", timeIndex);

        MiruInMemoryActivityIndex activityIndex = new MiruInMemoryActivityIndex();
        exportHandles.put("activityIndex", activityIndex);

        MiruInMemoryIndex index = new MiruInMemoryIndex();
        exportHandles.put("index", index);

        MiruInMemoryField[] fields = new MiruInMemoryField[schema.fieldCount()];
        for (int fieldId = 0; fieldId < fields.length; fieldId++) {
            fields[fieldId] = new MiruInMemoryField(schema.getFieldDefinition(fieldId), new ConcurrentHashMap<MiruTermId, MiruFieldIndexKey>(), index);
            exportHandles.put("field" + fieldId, fields[fieldId]);
        }

        MiruFields fieldIndex = new MiruFields(fields, index);
        MiruAuthzUtils authzUtils = new MiruAuthzUtils(bitsetBufferSize);
        MiruFilterUtils filterUtils = new MiruFilterUtils();

        //TODO share the cache?
        Cache<VersionedAuthzExpression, EWAHCompressedBitmap> authzCache = CacheBuilder.newBuilder()
                .maximumSize(partitionAuthzCacheSize)
                .expireAfterAccess(1, TimeUnit.MINUTES) //TODO should be adjusted with respect to tuning GC (prevent promotion from eden space)
                .build();
        MiruInMemoryAuthzIndex authzIndex = new MiruInMemoryAuthzIndex(new MiruAuthzCache(authzCache, stringInterner, authzUtils));
        exportHandles.put("authzIndex", authzIndex);

        MiruInMemoryRemovalIndex removalIndex = new MiruInMemoryRemovalIndex(new EWAHCompressedBitmap());
        exportHandles.put("removalIndex", removalIndex);

        MiruInMemoryUnreadTrackingIndex unreadTrackingIndex = new MiruInMemoryUnreadTrackingIndex();
        exportHandles.put("unreadTrackingIndex", unreadTrackingIndex);

        MiruInMemoryInboxIndex inboxIndex = new MiruInMemoryInboxIndex();
        exportHandles.put("inboxIndex", inboxIndex);

        MiruIndexStream indexStream = new MiruIndexStream(schema, activityIndex, fieldIndex, authzIndex, removalIndex, activityInterner);

        StripingLocksProvider<MiruStreamId> streamLocks = new StripingLocksProvider<>(64);
        MiruReadTrackStream readTrackStream = new MiruReadTrackStream(filterUtils, schema, fieldIndex, timeIndex, unreadTrackingIndex,
                executorService, bitsetBufferSize, streamLocks);

        MiruQueryStream queryStream = new MiruQueryStream(executorService,
                schema, timeIndex, activityIndex, fieldIndex, authzIndex, removalIndex, unreadTrackingIndex, inboxIndex,
                readTrackStream, readTrackingWALReader, streamLocks);

        return new MiruStream(indexStream, queryStream, readTrackStream, timeIndex, Optional.<ChunkStore>absent()).exportable(exportHandles);
    }

    private MiruStream allocateTransient() throws Exception {
        Map<String, BulkExport<?>> exportHandles = Maps.newHashMap();

        MiruResourcePartitionIdentifier identifier = transientResourceLocator.acquire();

        MiruInMemoryTimeIndex timeIndex = new MiruInMemoryTimeIndex(Optional.<MiruInMemoryTimeIndex.TimeOrderAnomalyStream>absent());
        exportHandles.put("timeIndex", timeIndex);

        File chunkFile = transientResourceLocator.getChunkFile(identifier, "activity");
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunkFile.getAbsolutePath(), transientResourceLocator.getInitialChunkSize(), true);

        MiruTransientActivityIndex activityIndex = new MiruTransientActivityIndex(
                transientResourceLocator.getMapDirectory(identifier, "activity"),
                transientResourceLocator.getSwapDirectory(identifier, "activity"),
                chunkStore,
                objectMapper);
        exportHandles.put("activityIndex", activityIndex);

        MiruInMemoryIndex index = new MiruInMemoryIndex();
        exportHandles.put("index", index);

        MiruTransientField[] fields = new MiruTransientField[schema.fieldCount()];
        for (int fieldId = 0; fieldId < fields.length; fieldId++) {
            fields[fieldId] = new MiruTransientField(
                    schema.getFieldDefinition(fieldId),
                    index,
                    transientResourceLocator.getMapDirectory(identifier, "field" + fieldId));
            exportHandles.put("field" + fieldId, fields[fieldId]);
        }

        MiruFields fieldIndex = new MiruFields(fields, index);
        MiruAuthzUtils authzUtils = new MiruAuthzUtils(bitsetBufferSize);
        MiruFilterUtils filterUtils = new MiruFilterUtils();

        //TODO share the cache?
        Cache<VersionedAuthzExpression, EWAHCompressedBitmap> authzCache = CacheBuilder.newBuilder()
                .maximumSize(partitionAuthzCacheSize)
                .expireAfterAccess(1, TimeUnit.MINUTES) //TODO should be adjusted with respect to tuning GC (prevent promotion from eden space)
                .build();
        MiruInMemoryAuthzIndex authzIndex = new MiruInMemoryAuthzIndex(new MiruAuthzCache(authzCache, stringInterner, authzUtils));
        exportHandles.put("authzIndex", authzIndex);

        MiruInMemoryRemovalIndex removalIndex = new MiruInMemoryRemovalIndex(new EWAHCompressedBitmap());
        exportHandles.put("removalIndex", removalIndex);

        MiruInMemoryUnreadTrackingIndex unreadTrackingIndex = new MiruInMemoryUnreadTrackingIndex();
        exportHandles.put("unreadTrackingIndex", unreadTrackingIndex);

        MiruInMemoryInboxIndex inboxIndex = new MiruInMemoryInboxIndex();
        exportHandles.put("inboxIndex", inboxIndex);

        MiruIndexStream indexStream = new MiruIndexStream(schema, activityIndex, fieldIndex, authzIndex, removalIndex, activityInterner);

        StripingLocksProvider<MiruStreamId> streamLocks = new StripingLocksProvider<>(64);
        MiruReadTrackStream readTrackStream = new MiruReadTrackStream(filterUtils, schema, fieldIndex, timeIndex, unreadTrackingIndex,
                executorService, bitsetBufferSize, streamLocks);

        MiruQueryStream queryStream = new MiruQueryStream(executorService,
                schema, timeIndex, activityIndex, fieldIndex, authzIndex, removalIndex, unreadTrackingIndex, inboxIndex,
                readTrackStream, readTrackingWALReader, streamLocks);

        return new MiruStream(indexStream, queryStream, readTrackStream, timeIndex, Optional.of(chunkStore))
                .exportable(exportHandles)
                .withTransientResource(identifier);
    }

    private MiruStream allocateMemMapped(MiruPartitionCoord coord) throws Exception {
        //TODO refactor OnDisk impls to take a shared VariableKeySizeFileBackedKeyedStore and a prefixed KeyProvider

        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);

        File memMap = diskResourceLocator.getFilerFile(identifier, "memMap");
        memMap.createNewFile();

        File chunkFile = diskResourceLocator.getChunkFile(identifier, "stream");
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunkFile.getAbsolutePath(), diskResourceLocator.getInitialChunkSize(), true);

        Map<String, BulkImport<?>> importHandles = Maps.newHashMap();

        MiruOnDiskTimeIndex timeIndex = new MiruOnDiskTimeIndex(
                new MemMappedFilerProvider(identifier, "timeIndex"),
                diskResourceLocator.getMapDirectory(identifier, "timestampToIndex"));
        importHandles.put("timeIndex", timeIndex);

        MiruMemMappedActivityIndex activityIndex = new MiruMemMappedActivityIndex(
                new MemMappedFilerProvider(identifier, "activity"),
                diskResourceLocator.getMapDirectory(identifier, "activity"),
                diskResourceLocator.getSwapDirectory(identifier, "activity"),
                chunkStore,
                objectMapper);
        importHandles.put("activityIndex", activityIndex);

        MiruOnDiskIndex index = new MiruOnDiskIndex(
                diskResourceLocator.getMapDirectory(identifier, "index"),
                diskResourceLocator.getSwapDirectory(identifier, "index"),
                chunkStore);
        importHandles.put("index", index);

        MiruOnDiskField[] fields = new MiruOnDiskField[schema.fieldCount()];
        for (int fieldId = 0; fieldId < fields.length; fieldId++) {
            fields[fieldId] = new MiruOnDiskField(
                    schema.getFieldDefinition(fieldId),
                    index,
                    diskResourceLocator.getMapDirectory(identifier, "field-" + fieldId));
            importHandles.put("field" + fieldId, fields[fieldId]);
        }

        MiruFields fieldIndex = new MiruFields(fields, index);
        MiruAuthzUtils authzUtils = new MiruAuthzUtils(bitsetBufferSize);
        MiruFilterUtils filterUtils = new MiruFilterUtils();

        //TODO share the cache?
        Cache<VersionedAuthzExpression, EWAHCompressedBitmap> authzCache = CacheBuilder.newBuilder()
                .maximumSize(partitionAuthzCacheSize)
                .expireAfterAccess(1, TimeUnit.MINUTES) //TODO should be adjusted with respect to tuning GC (prevent promotion from eden space)
                .build();
        MiruOnDiskAuthzIndex authzIndex = new MiruOnDiskAuthzIndex(
                diskResourceLocator.getMapDirectory(identifier, "authz"),
                diskResourceLocator.getSwapDirectory(identifier, "authz"),
                chunkStore,
                new MiruAuthzCache(authzCache, stringInterner, authzUtils));
        importHandles.put("authzIndex", authzIndex);

        MiruOnDiskRemovalIndex removalIndex = new MiruOnDiskRemovalIndex(new RandomAccessSwappableFiler(
                diskResourceLocator.getFilerFile(identifier, "removal")));
        importHandles.put("removalIndex", removalIndex);

        MiruOnDiskUnreadTrackingIndex unreadTrackingIndex = new MiruOnDiskUnreadTrackingIndex(
                diskResourceLocator.getMapDirectory(identifier, "unread"),
                diskResourceLocator.getSwapDirectory(identifier, "unread"),
                chunkStore);
        importHandles.put("unreadTrackingIndex", unreadTrackingIndex);

        MiruOnDiskInboxIndex inboxIndex = new MiruOnDiskInboxIndex(
                diskResourceLocator.getMapDirectory(identifier, "inbox"),
                diskResourceLocator.getSwapDirectory(identifier, "inbox"),
                chunkStore);
        importHandles.put("inboxIndex", inboxIndex);

        MiruIndexStream indexStream = new MiruIndexStream(schema, activityIndex, fieldIndex, authzIndex, removalIndex, activityInterner);

        StripingLocksProvider<MiruStreamId> streamLocks = new StripingLocksProvider<>(64);
        MiruReadTrackStream readTrackStream = new MiruReadTrackStream(filterUtils, schema, fieldIndex, timeIndex, unreadTrackingIndex,
                executorService, bitsetBufferSize, streamLocks);

        MiruQueryStream queryStream = new MiruQueryStream(executorService,
                schema, timeIndex, activityIndex, fieldIndex, authzIndex, removalIndex, unreadTrackingIndex, inboxIndex,
                readTrackStream, readTrackingWALReader, streamLocks);

        return new MiruStream(indexStream, queryStream, readTrackStream, timeIndex, Optional.of(chunkStore)).importable(importHandles);
    }

    private MiruStream allocateOnDisk(MiruPartitionCoord coord) throws Exception {
        //TODO refactor OnDisk impls to take a shared VariableKeySizeFileBackedKeyedStore and a prefixed KeyProvider

        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        Map<String, BulkImport<?>> importHandles = Maps.newHashMap();

        File onDisk = diskResourceLocator.getFilerFile(identifier, "onDisk");
        onDisk.createNewFile();

        File chunkFile = diskResourceLocator.getChunkFile(identifier, "stream");
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunkFile.getAbsolutePath(), diskResourceLocator.getInitialChunkSize(), true);

        MiruOnDiskTimeIndex timeIndex = new MiruOnDiskTimeIndex(
                new OnDiskFilerProvider(identifier, "timeIndex"),
                diskResourceLocator.getMapDirectory(identifier, "timestampToIndex"));
        importHandles.put("timeIndex", timeIndex);

        MiruMemMappedActivityIndex activityIndex = new MiruMemMappedActivityIndex(
                new MemMappedFilerProvider(identifier, "activity"),
                diskResourceLocator.getMapDirectory(identifier, "activity"),
                diskResourceLocator.getSwapDirectory(identifier, "activity"),
                chunkStore,
                objectMapper);
        importHandles.put("activityIndex", activityIndex);

        MiruOnDiskIndex index = new MiruOnDiskIndex(
                diskResourceLocator.getMapDirectory(identifier, "index"),
                diskResourceLocator.getSwapDirectory(identifier, "index"),
                chunkStore);
        importHandles.put("index", index);

        MiruOnDiskField[] fields = new MiruOnDiskField[schema.fieldCount()];
        for (int fieldId = 0; fieldId < fields.length; fieldId++) {
            fields[fieldId] = new MiruOnDiskField(schema.getFieldDefinition(fieldId),
                    index,
                    diskResourceLocator.getMapDirectory(identifier, "field-" + fieldId));
            importHandles.put("field" + fieldId, fields[fieldId]);
        }

        MiruFields fieldIndex = new MiruFields(fields, index);
        MiruAuthzUtils authzUtils = new MiruAuthzUtils(bitsetBufferSize);
        MiruFilterUtils filterUtils = new MiruFilterUtils();

        Cache<VersionedAuthzExpression, EWAHCompressedBitmap> authzCache = CacheBuilder.newBuilder()
                .maximumSize(partitionAuthzCacheSize)
                .expireAfterAccess(1, TimeUnit.MINUTES) //TODO should be adjusted with respect to tuning GC (prevent promotion from eden space)
                .build();
        MiruOnDiskAuthzIndex authzIndex = new MiruOnDiskAuthzIndex(
                diskResourceLocator.getMapDirectory(identifier, "authz"),
                diskResourceLocator.getSwapDirectory(identifier, "authz"),
                chunkStore,
                new MiruAuthzCache(authzCache, stringInterner, authzUtils));
        importHandles.put("authzIndex", authzIndex);

        MiruOnDiskRemovalIndex removalIndex = new MiruOnDiskRemovalIndex(new RandomAccessSwappableFiler(
                diskResourceLocator.getFilerFile(identifier, "removal")));
        importHandles.put("removalIndex", removalIndex);

        MiruOnDiskUnreadTrackingIndex unreadTrackingIndex = new MiruOnDiskUnreadTrackingIndex(
                diskResourceLocator.getMapDirectory(identifier, "unread"),
                diskResourceLocator.getSwapDirectory(identifier, "unread"),
                chunkStore);
        importHandles.put("unreadTrackingIndex", unreadTrackingIndex);

        MiruOnDiskInboxIndex inboxIndex = new MiruOnDiskInboxIndex(
                diskResourceLocator.getMapDirectory(identifier, "inbox"),
                diskResourceLocator.getSwapDirectory(identifier, "inbox"),
                chunkStore);
        importHandles.put("inboxIndex", inboxIndex);

        MiruIndexStream indexStream = new MiruIndexStream(schema, activityIndex, fieldIndex, authzIndex, removalIndex, activityInterner);

        StripingLocksProvider<MiruStreamId> streamLocks = new StripingLocksProvider<>(64);
        MiruReadTrackStream readTrackStream = new MiruReadTrackStream(filterUtils, schema, fieldIndex, timeIndex, unreadTrackingIndex,
                executorService, bitsetBufferSize, streamLocks);

        MiruQueryStream queryStream = new MiruQueryStream(executorService,
                schema, timeIndex, activityIndex, fieldIndex, authzIndex, removalIndex, unreadTrackingIndex, inboxIndex,
                readTrackStream, readTrackingWALReader, streamLocks);

        return new MiruStream(indexStream, queryStream, readTrackStream, timeIndex, Optional.of(chunkStore)).importable(importHandles);
    }

    public MiruStream copyMemMapped(MiruPartitionCoord coord, MiruStream from) throws Exception {
        return copy(from, allocateMemMapped(coord));
    }

    public MiruStream copyToDisk(MiruPartitionCoord coord, MiruStream from) throws Exception {
        return copy(from, allocateOnDisk(coord));
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    private MiruStream copy(MiruStream from, MiruStream to) throws Exception {
        Map<String, BulkImport<?>> importHandles = to.getImportHandles();
        for (Map.Entry<String, BulkExport<?>> entry : from.getExportHandles().entrySet()) {
            String key = entry.getKey();
            BulkExport<?> bulkExport = entry.getValue();
            BulkImport<?> bulkImport = importHandles.get(key);
            if (bulkImport != null) {
                bulkImport.bulkImport((BulkExport) bulkExport);
            } else {
                log.warn("Missing bulk importer for {}", key);
            }
        }
        return to;
    }

    public void cleanDisk(MiruPartitionCoord coord) throws IOException {
        diskResourceLocator.clean(new MiruPartitionCoordIdentifier(coord));
    }

    public void close(MiruStream stream) {
        MiruQueryStream queryStream = stream.getQueryStream();
        queryStream.activityIndex.close();
        queryStream.authzIndex.close();
        queryStream.timeIndex.close();
        queryStream.unreadTrackingIndex.close();
        queryStream.inboxIndex.close();

        Optional<? extends MiruResourcePartitionIdentifier> transientResource = stream.getTransientResource();
        if (transientResource.isPresent()) {
            transientResourceLocator.release(transientResource.get());
        }
    }

    private boolean checkMarkedStorage(MiruPartitionCoord coord, MiruBackingStorage storage) throws IOException {
        File file = diskResourceLocator.getFilerFile(new MiruPartitionCoordIdentifier(coord), storage.name());
        if (file.exists()) {
            if (storage == MiruBackingStorage.mem_mapped) {
                return checkMemMapped(coord);
            } else if (storage == MiruBackingStorage.disk) {
                return checkOnDisk(coord);
            } else {
                return true;
            }
        }
        return false;
    }

    private boolean checkMemMapped(MiruPartitionCoord coord) throws IOException {
        List<String> mapDirectories = Lists.newArrayList("activity", "index", "authz", "unread", "inbox", "timestampToIndex");
        for (int i = 0; i < schema.fieldCount(); i++) {
            mapDirectories.add("field-" + i);
        }
        return diskResourceAnalyzer.checkExists(
                diskResourceLocator.getPartitionPath(new MiruPartitionCoordIdentifier(coord)),
                Lists.newArrayList("timeIndex", "activity", "removal"),
                mapDirectories,
                Lists.newArrayList("stream"));
    }

    private boolean checkOnDisk(MiruPartitionCoord coord) throws IOException {
        List<String> mapDirectories = Lists.newArrayList("activity", "index", "authz", "unread", "inbox", "timestampToIndex");
        for (int i = 0; i < schema.fieldCount(); i++) {
            mapDirectories.add("field-" + i);
        }
        return diskResourceAnalyzer.checkExists(
                diskResourceLocator.getPartitionPath(new MiruPartitionCoordIdentifier(coord)),
                Lists.newArrayList("timeIndex", "activity", "removal"),
                mapDirectories,
                Lists.newArrayList("stream"));
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
