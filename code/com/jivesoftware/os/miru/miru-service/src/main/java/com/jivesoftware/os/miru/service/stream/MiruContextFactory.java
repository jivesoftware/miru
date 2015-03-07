package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.jivesoftware.os.filer.chunk.store.transaction.MapBackedKeyedFPIndex;
import com.jivesoftware.os.filer.chunk.store.transaction.TxCog;
import com.jivesoftware.os.filer.chunk.store.transaction.TxCogs;
import com.jivesoftware.os.filer.chunk.store.transaction.TxNamedMapOfFiler;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.filer.io.primative.LongIntKeyValueMarshaller;
import com.jivesoftware.os.filer.keyed.store.TxKeyValueStore;
import com.jivesoftware.os.filer.keyed.store.TxKeyedFilerStore;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaUnvailableException;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndexProvider;
import com.jivesoftware.os.miru.plugin.index.MiruInboxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruRemovalIndex;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.index.MiruUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.index.KeyedFilerProvider;
import com.jivesoftware.os.miru.service.index.MiruInternalActivityMarshaller;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import com.jivesoftware.os.miru.service.index.auth.VersionedAuthzExpression;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaActivityIndex;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaAuthzIndex;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaFieldIndex;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaInboxIndex;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaInvertedIndex;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaRemovalIndex;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaSipIndex;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaTimeIndex;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.index.filer.MiruFilerActivityIndex;
import com.jivesoftware.os.miru.service.index.filer.MiruFilerAuthzIndex;
import com.jivesoftware.os.miru.service.index.filer.MiruFilerFieldIndex;
import com.jivesoftware.os.miru.service.index.filer.MiruFilerInboxIndex;
import com.jivesoftware.os.miru.service.index.filer.MiruFilerRemovalIndex;
import com.jivesoftware.os.miru.service.index.filer.MiruFilerSipIndex;
import com.jivesoftware.os.miru.service.index.filer.MiruFilerTimeIndex;
import com.jivesoftware.os.miru.service.index.filer.MiruFilerUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.locator.MiruPartitionCoordIdentifier;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruResourcePartitionIdentifier;
import com.jivesoftware.os.miru.service.stream.allocator.MiruChunkAllocator;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan
 */
public class MiruContextFactory {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private static final byte[] GENERIC_FILER_TIME_INDEX_KEY = new byte[]{0};
    private static final byte[] GENERIC_FILER_SIP_INDEX_KEY = new byte[]{1};

    private final TxCogs cogs;
    private final MiruSchemaProvider schemaProvider;
    private final MiruTermComposer termComposer;
    private final MiruActivityInternExtern activityInternExtern;
    private final MiruReadTrackingWALReader readTrackingWALReader;
    private final Map<MiruBackingStorage, MiruChunkAllocator> allocators;
    private final MiruResourceLocator diskResourceLocator;
    private final MiruBackingStorage defaultStorage;
    private final int partitionAuthzCacheSize;
    private final Cache<MiruFieldIndex.IndexKey, Optional<?>> fieldIndexCache;
    private final AtomicLong fieldIndexIdProvider;
    private final StripingLocksProvider<MiruTermId> fieldIndexStripingLocksProvider;
    private final StripingLocksProvider<MiruStreamId> streamStripingLocksProvider;
    private final StripingLocksProvider<String> authzStripingLocksProvider;

    public MiruContextFactory(TxCogs cogs,
        MiruSchemaProvider schemaProvider,
        MiruTermComposer termComposer,
        MiruActivityInternExtern activityInternExtern,
        MiruReadTrackingWALReader readTrackingWALReader,
        Map<MiruBackingStorage, MiruChunkAllocator> allocators,
        MiruResourceLocator diskResourceLocator,
        MiruBackingStorage defaultStorage,
        int partitionAuthzCacheSize,
        Cache<MiruFieldIndex.IndexKey, Optional<?>> fieldIndexCache,
        AtomicLong fieldIndexIdProvider,
        StripingLocksProvider<MiruTermId> fieldIndexStripingLocksProvider,
        StripingLocksProvider<MiruStreamId> streamStripingLocksProvider,
        StripingLocksProvider<String> authzStripingLocksProvider) {

        this.cogs = cogs;
        this.schemaProvider = schemaProvider;
        this.termComposer = termComposer;
        this.activityInternExtern = activityInternExtern;
        this.readTrackingWALReader = readTrackingWALReader;
        this.allocators = allocators;
        this.diskResourceLocator = diskResourceLocator;
        this.defaultStorage = defaultStorage;
        this.partitionAuthzCacheSize = partitionAuthzCacheSize;
        this.fieldIndexCache = fieldIndexCache;
        this.fieldIndexIdProvider = fieldIndexIdProvider;
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
        // check for schema first
        MiruSchema schema = schemaProvider.getSchema(coord.tenantId);
        int seed = coord.hashCode();
        TxCog<Integer, MapBackedKeyedFPIndex, ChunkFiler> skyhookCog = cogs.getSkyhookCog(seed);
        KeyedFilerStore<Long, Void> genericFilerStore = new TxKeyedFilerStore<>(cogs, seed, chunkStores, keyBytes("generic"), false,
            TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
            TxNamedMapOfFiler.CHUNK_FILER_OPENER,
            TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
            TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER);

        MiruTimeIndex timeIndex = new MiruDeltaTimeIndex(new MiruFilerTimeIndex(
            Optional.<MiruFilerTimeIndex.TimeOrderAnomalyStream>absent(),
            new KeyedFilerProvider<>(genericFilerStore, GENERIC_FILER_TIME_INDEX_KEY),
            new TxKeyValueStore<>(skyhookCog, seed, chunkStores,
                new LongIntKeyValueMarshaller(),
                keyBytes("timeIndex-timestamps"),
                8, false, 4, false)));

        TxKeyedFilerStore<Long, Void> activityFilerStore = new TxKeyedFilerStore<>(cogs, seed, chunkStores, keyBytes("activityIndex"), false,
            TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
            TxNamedMapOfFiler.CHUNK_FILER_OPENER,
            TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
            TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER);

        MiruActivityIndex activityIndex = new MiruDeltaActivityIndex(
            new MiruFilerActivityIndex(
                activityFilerStore,
                new MiruInternalActivityMarshaller(),
                new KeyedFilerProvider<>(activityFilerStore, keyBytes("activityIndex-size"))));

        @SuppressWarnings("unchecked")
        MiruFieldIndex<BM>[] fieldIndexes = new MiruFieldIndex[MiruFieldType.values().length];
        for (MiruFieldType fieldType : MiruFieldType.values()) {
            @SuppressWarnings("unchecked")
            KeyedFilerStore<Long, Void>[] indexes = new KeyedFilerStore[schema.fieldCount()];
            long[] indexIds = new long[schema.fieldCount()];
            for (MiruFieldDefinition fieldDefinition : schema.getFieldDefinitions()) {
                int fieldId = fieldDefinition.fieldId;
                if (fieldType == MiruFieldType.latest && fieldDefinition.type != MiruFieldDefinition.Type.singleTermIndexLatest
                    || fieldType == MiruFieldType.pairedLatest && schema.getPairedLatestFieldDefinitions(fieldId).isEmpty()
                    || fieldType == MiruFieldType.bloom && schema.getBloomFieldDefinitions(fieldId).isEmpty()) {
                    indexes[fieldId] = null;
                } else {
                    boolean lexOrderKeys = (fieldDefinition.prefix.type != MiruFieldDefinition.Prefix.Type.none);
                    indexes[fieldId] = new TxKeyedFilerStore<>(cogs, seed, chunkStores, keyBytes("field-" + fieldType.name() + "-" + fieldId), lexOrderKeys,
                        TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
                        TxNamedMapOfFiler.CHUNK_FILER_OPENER,
                        TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
                        TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER);
                }
                indexIds[fieldId] = fieldIndexIdProvider.incrementAndGet();
            }
            fieldIndexes[fieldType.getIndex()] = new MiruDeltaFieldIndex<>(
                bitmaps,
                indexIds,
                new MiruFilerFieldIndex<>(bitmaps, indexIds, indexes, fieldIndexStripingLocksProvider),
                schema.fieldCount(),
                fieldIndexCache);
        }
        MiruFieldIndexProvider<BM> fieldIndexProvider = new MiruFieldIndexProvider<>(fieldIndexes);

        MiruSipIndex sipIndex = new MiruDeltaSipIndex(new MiruFilerSipIndex(new KeyedFilerProvider<>(genericFilerStore, GENERIC_FILER_SIP_INDEX_KEY)));

        MiruAuthzUtils<BM> authzUtils = new MiruAuthzUtils<>(bitmaps);

        Cache<VersionedAuthzExpression, BM> authzCache = CacheBuilder.newBuilder()
            .maximumSize(partitionAuthzCacheSize)
            .expireAfterAccess(1, TimeUnit.MINUTES) //TODO should be adjusted with respect to tuning GC (prevent promotion from eden space)
            .build();
        MiruAuthzCache<BM> miruAuthzCache = new MiruAuthzCache<>(bitmaps, authzCache, activityInternExtern, authzUtils);

        long authzIndexId = fieldIndexIdProvider.incrementAndGet();
        MiruAuthzIndex<BM> authzIndex = new MiruDeltaAuthzIndex<>(bitmaps,
            authzIndexId,
            miruAuthzCache,
            new MiruFilerAuthzIndex<>(
                bitmaps,
                authzIndexId,
                new TxKeyedFilerStore<>(cogs, seed, chunkStores, keyBytes("authzIndex"), false,
                    TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
                    TxNamedMapOfFiler.CHUNK_FILER_OPENER,
                    TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
                    TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER),
                miruAuthzCache,
                authzStripingLocksProvider),
            fieldIndexCache);

        long removalIndexId = fieldIndexIdProvider.incrementAndGet();
        MiruRemovalIndex<BM> removalIndex = new MiruDeltaRemovalIndex<>(
            bitmaps,
            fieldIndexCache,
            removalIndexId,
            new byte[]{0},
            new MiruFilerRemovalIndex<>(
                bitmaps,
                removalIndexId,
                new TxKeyedFilerStore<>(cogs, seed, chunkStores, keyBytes("removalIndex"), false,
                    TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
                    TxNamedMapOfFiler.CHUNK_FILER_OPENER,
                    TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
                    TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER),
                new byte[]{0},
                -1,
                new Object()),
            new MiruDeltaInvertedIndex.Delta<BM>());

        long unreadTrackingIndexId = fieldIndexIdProvider.incrementAndGet();
        MiruUnreadTrackingIndex<BM> unreadTrackingIndex = new MiruDeltaUnreadTrackingIndex<>(
            bitmaps,
            unreadTrackingIndexId,
            new MiruFilerUnreadTrackingIndex<>(
                bitmaps,
                unreadTrackingIndexId,
                new TxKeyedFilerStore<>(cogs, seed, chunkStores, keyBytes("unreadTrackingIndex"), false,
                    TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
                    TxNamedMapOfFiler.CHUNK_FILER_OPENER,
                    TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
                    TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER),
                streamStripingLocksProvider),
            fieldIndexCache);

        long inboxIndexId = fieldIndexIdProvider.incrementAndGet();
        MiruInboxIndex<BM> inboxIndex = new MiruDeltaInboxIndex<>(
            bitmaps,
            inboxIndexId,
            new MiruFilerInboxIndex<>(
                bitmaps,
                inboxIndexId,
                new TxKeyedFilerStore<>(cogs, seed, chunkStores, keyBytes("inboxIndex"), false,
                    TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
                    TxNamedMapOfFiler.CHUNK_FILER_OPENER,
                    TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
                    TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER),
                streamStripingLocksProvider),
            fieldIndexCache);

        StripingLocksProvider<MiruStreamId> streamLocks = new StripingLocksProvider<>(64);

        return new MiruContext<>(schema,
            termComposer,
            timeIndex,
            activityIndex,
            fieldIndexProvider,
            sipIndex,
            authzIndex,
            removalIndex,
            unreadTrackingIndex,
            inboxIndex,
            readTrackingWALReader,
            activityInternExtern,
            streamLocks,
            chunkStores);
    }

    public <BM> MiruContext<BM> copy(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord, MiruContext<BM> from, MiruBackingStorage toStorage) throws Exception {

        ChunkStore[] fromChunks = from.chunkStores;
        ChunkStore[] toChunks = getAllocator(toStorage).allocateChunkStores(coord);
        if (fromChunks.length != toChunks.length) {
            throw new IllegalArgumentException("The number of from chunks:" + fromChunks.length + " must equal the number of to chunks:" + toChunks.length);
        }
        for (int i = 0; i < fromChunks.length; i++) {
            fromChunks[i].copyTo(toChunks[i]);
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

    public void cleanDisk(MiruPartitionCoord coord) throws IOException {
        diskResourceLocator.clean(new MiruPartitionCoordIdentifier(coord));
    }

    public <BM> void close(MiruContext<BM> context, MiruBackingStorage storage) {
        context.activityIndex.close();
        context.authzIndex.close();
        context.timeIndex.close();
        context.unreadTrackingIndex.close();
        context.inboxIndex.close();

        getAllocator(storage).close(context.chunkStores);
    }

    public <BM> void releaseCaches(MiruContext<BM> context, MiruBackingStorage storage) throws IOException {
        for (ChunkStore chunkStore : context.chunkStores) {
            chunkStore.rollCache();
        }
    }

    private byte[] keyBytes(String key) {
        return key.getBytes(Charsets.UTF_8);
    }
}
