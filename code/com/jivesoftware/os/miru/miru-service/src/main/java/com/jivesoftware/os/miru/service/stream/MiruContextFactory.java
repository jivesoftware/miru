package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.jivesoftware.os.filer.chunk.store.ChunkStore;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.RandomAccessFiler;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.primative.LongIntKeyValueMarshaller;
import com.jivesoftware.os.filer.keyed.store.KeyedFilerStore;
import com.jivesoftware.os.filer.keyed.store.TxKeyValueStore;
import com.jivesoftware.os.filer.keyed.store.TxKeyedFilerStore;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndexProvider;
import com.jivesoftware.os.miru.plugin.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.plugin.schema.MiruSchemaUnvailableException;
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
import com.jivesoftware.os.miru.service.index.disk.MiruFilerTimeIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruFilerUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.locator.MiruHybridResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruPartitionCoordIdentifier;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruResourcePartitionIdentifier;
import com.jivesoftware.os.miru.service.stream.allocator.MiruChunkAllocator;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author jonathan
 */
public class MiruContextFactory {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();
    private static final String DISK_FORMAT_VERSION = "version-12";

    private final MiruSchemaProvider schemaProvider;
    private final MiruActivityInternExtern activityInternExtern;
    private final MiruReadTrackingWALReader readTrackingWALReader;
    private final Map<MiruBackingStorage, MiruChunkAllocator> allocators;
    private final MiruResourceLocator diskResourceLocator;
    private final MiruHybridResourceLocator hybridResourceLocator;
    private final MiruBackingStorage defaultStorage;
    private final int partitionAuthzCacheSize;
    private final StripingLocksProvider<MiruTermId> fieldIndexStripingLocksProvider;
    private final StripingLocksProvider<MiruStreamId> streamStripingLocksProvider;
    private final StripingLocksProvider<String> authzStripingLocksProvider;

    public MiruContextFactory(MiruSchemaProvider schemaProvider,
        MiruActivityInternExtern activityInternExtern,
        MiruReadTrackingWALReader readTrackingWALReader,
        Map<MiruBackingStorage, MiruChunkAllocator> allocators,
        MiruResourceLocator diskResourceLocator,
        MiruHybridResourceLocator transientResourceLocator,
        MiruBackingStorage defaultStorage,
        int partitionAuthzCacheSize,
        StripingLocksProvider<MiruTermId> fieldIndexStripingLocksProvider,
        StripingLocksProvider<MiruStreamId> streamStripingLocksProvider,
        StripingLocksProvider<String> authzStripingLocksProvider) {
        this.schemaProvider = schemaProvider;
        this.activityInternExtern = activityInternExtern;
        this.readTrackingWALReader = readTrackingWALReader;
        this.allocators = allocators;
        this.diskResourceLocator = diskResourceLocator;
        this.hybridResourceLocator = transientResourceLocator;
        this.defaultStorage = defaultStorage;
        this.partitionAuthzCacheSize = partitionAuthzCacheSize;
        this.fieldIndexStripingLocksProvider = fieldIndexStripingLocksProvider;
        this.streamStripingLocksProvider = streamStripingLocksProvider;
        this.authzStripingLocksProvider = authzStripingLocksProvider;
    }

    public MiruBackingStorage findBackingStorage(MiruPartitionCoord coord) throws Exception {
        try {
            for (MiruBackingStorage storage : MiruBackingStorage.values()) {
                if (checkMarkedStorage(coord, storage)) {
                    return storage;
                }
            }
        } catch (MiruSchemaUnvailableException e) {
            log.warn("Schema not registered for tenant {}, using default storage", coord.tenantId);
        }
        return defaultStorage;
    }

    private MiruChunkAllocator getAllocator(MiruBackingStorage storage) {
        MiruChunkAllocator allocator = allocators.get(storage);
        if (allocator != null) {
            return allocator;
        } else {
            throw new RuntimeException("backingStorage:" + storage + " is unsupported.");
        }
    }

    public <BM> MiruContext<BM> allocate(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord, MiruBackingStorage storage) throws Exception {
        ChunkStore[] chunkStores = getAllocator(storage).allocateChunkStores(coord);
        return allocate(bitmaps, coord, chunkStores);
    }

    private <BM> MiruContext<BM> allocate(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord, ChunkStore[] chunkStores) throws Exception {
        //TODO refactor OnDisk impls to take a shared VariableKeySizeFileBackedKeyedStore and a prefixed KeyProvider

        // check for schema first
        MiruSchema schema = schemaProvider.getSchema(coord.tenantId);

        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);

        File versionFile = diskResourceLocator.getFilerFile(identifier, DISK_FORMAT_VERSION);
        versionFile.createNewFile();

        KeyedFilerStore timeIndexFilerStore = new TxKeyedFilerStore(chunkStores, keyBytes("timeIndex-index"));
        MiruFilerTimeIndex timeIndex = new MiruFilerTimeIndex(
            Optional.<MiruFilerTimeIndex.TimeOrderAnomalyStream>absent(),
            new KeyedFilerProvider(timeIndexFilerStore, new byte[]{0}),
            new TxKeyValueStore<>(chunkStores,
                new LongIntKeyValueMarshaller(),
                keyBytes("timeIndex-timestamps"),
                8, false, 4, false));

        TxKeyedFilerStore activityFilerStore = new TxKeyedFilerStore(chunkStores, keyBytes("activityIndex"));
        MiruFilerActivityIndex activityIndex = new MiruFilerActivityIndex(
            activityFilerStore,
            new MiruInternalActivityMarshaller(),
            new KeyedFilerProvider(activityFilerStore, keyBytes("activityIndex-size")));

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
                    indexes[fieldId] = new TxKeyedFilerStore(chunkStores, keyBytes("field-" + fieldType.name() + "-" + fieldId));
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
            new TxKeyedFilerStore(chunkStores, keyBytes("authzIndex")),
            new MiruAuthzCache<>(bitmaps, authzCache, activityInternExtern, authzUtils),
            authzStripingLocksProvider);

        MiruFilerRemovalIndex<BM> removalIndex = new MiruFilerRemovalIndex<>(
            bitmaps,
            new TxKeyedFilerStore(chunkStores, keyBytes("removalIndex")),
            new byte[]{0},
            -1,
            new Object());

        MiruFilerUnreadTrackingIndex<BM> unreadTrackingIndex = new MiruFilerUnreadTrackingIndex<>(
            bitmaps,
            new TxKeyedFilerStore(chunkStores, keyBytes("unreadTrackingIndex")),
            streamStripingLocksProvider);

        MiruFilerInboxIndex<BM> inboxIndex = new MiruFilerInboxIndex<>(
            bitmaps,
            new TxKeyedFilerStore(chunkStores, keyBytes("inboxIndex")),
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
            Optional.<ChunkStore[]>absent(),
            Optional.<MiruResourcePartitionIdentifier>absent());
    }

    public <BM> MiruContext<BM> copy(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord, MiruContext<BM> from, MiruBackingStorage toStorage) throws Exception {

        ChunkStore[] toChunks = getAllocator(toStorage).allocateChunkStores(coord);
        if (from.chunkStores.isPresent()) {
            ChunkStore[] fromChunks = from.chunkStores.get();
            if (fromChunks.length != toChunks.length) {
                throw new IllegalArgumentException("The number of from chunks:" + fromChunks.length + " must equal the number of to chunks:" + toChunks.length);
            }
            for (int i = 0; i < fromChunks.length; i++) {
                fromChunks[i].copyTo(toChunks[i]);
            }
        }
        return allocate(bitmaps, coord, toChunks);
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

    private boolean checkMarkedStorage(MiruPartitionCoord coord, MiruBackingStorage storage) throws Exception {
        File file = diskResourceLocator.getFilerFile(new MiruPartitionCoordIdentifier(coord), storage.name());
        return file.exists() && getAllocator(storage).checkExists(coord);
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

    public void cleanDisk(MiruPartitionCoord coord) throws IOException {
        diskResourceLocator.clean(new MiruPartitionCoordIdentifier(coord));
    }

    public <BM> void close(MiruContext<BM> context, MiruBackingStorage storage) {
        context.activityIndex.close();
        context.authzIndex.close();
        context.timeIndex.close();
        context.unreadTrackingIndex.close();
        context.inboxIndex.close();

        if (context.transientResource.isPresent()) {
            hybridResourceLocator.release(context.transientResource.get());
        }

        if (context.chunkStores.isPresent()) {
            getAllocator(storage).close(context.chunkStores.get());
        }
    }

    public <BM> void releaseCaches(MiruContext<BM> context, MiruBackingStorage storage) throws IOException {
        if (context.chunkStores.isPresent()) {
            ChunkStore[] chunkStores = context.chunkStores.get();
            for (ChunkStore chunkStore : chunkStores) {
                chunkStore.rollCache();
            }
        }
    }

    private byte[] keyBytes(String key) {
        return key.getBytes(Charsets.UTF_8);
    }
}
