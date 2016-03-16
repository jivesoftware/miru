package com.jivesoftware.os.miru.service.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.jivesoftware.os.filer.chunk.store.transaction.MapBackedKeyedFPIndex;
import com.jivesoftware.os.filer.chunk.store.transaction.MapCreator;
import com.jivesoftware.os.filer.chunk.store.transaction.MapOpener;
import com.jivesoftware.os.filer.chunk.store.transaction.TxCog;
import com.jivesoftware.os.filer.chunk.store.transaction.TxCogs;
import com.jivesoftware.os.filer.chunk.store.transaction.TxMapGrower;
import com.jivesoftware.os.filer.chunk.store.transaction.TxNamedMapOfFiler;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.filer.io.map.MapContext;
import com.jivesoftware.os.filer.io.primative.LongIntKeyValueMarshaller;
import com.jivesoftware.os.filer.keyed.store.TxKeyValueStore;
import com.jivesoftware.os.filer.keyed.store.TxKeyedFilerStore;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Feature;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaUnvailableException;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.context.MiruContextConstants;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.MiruInterner;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndexProvider;
import com.jivesoftware.os.miru.plugin.index.MiruInboxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruRemovalIndex;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndex;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndexMarshaller;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.index.MiruUnreadTrackingIndex;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.miru.service.index.KeyedFilerProvider;
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
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
import com.jivesoftware.os.miru.service.partition.PartitionErrorTracker;
import com.jivesoftware.os.miru.service.stream.allocator.MiruChunkAllocator;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author jonathan
 */
public class MiruContextFactory<S extends MiruSipCursor<S>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final TxCogs persistentCogs;
    private final TxCogs transientCogs;
    private final MiruSchemaProvider schemaProvider;
    private final MiruTermComposer termComposer;
    private final MiruActivityInternExtern activityInternExtern;
    private final Map<MiruBackingStorage, MiruChunkAllocator> allocators;
    private final MiruSipIndexMarshaller<S> sipMarshaller;
    private final MiruResourceLocator diskResourceLocator;
    private final int partitionAuthzCacheSize;
    private final StripingLocksProvider<MiruTermId> fieldIndexStripingLocksProvider;
    private final StripingLocksProvider<MiruStreamId> streamStripingLocksProvider;
    private final StripingLocksProvider<String> authzStripingLocksProvider;
    private final PartitionErrorTracker partitionErrorTracker;
    private final MiruInterner<MiruTermId> termInterner;
    private final ObjectMapper objectMapper;

    public MiruContextFactory(TxCogs persistentCogs,
        TxCogs transientCogs,
        MiruSchemaProvider schemaProvider,
        MiruTermComposer termComposer,
        MiruActivityInternExtern activityInternExtern,
        Map<MiruBackingStorage, MiruChunkAllocator> allocators,
        MiruSipIndexMarshaller<S> sipMarshaller,
        MiruResourceLocator diskResourceLocator,
        int partitionAuthzCacheSize,
        StripingLocksProvider<MiruTermId> fieldIndexStripingLocksProvider,
        StripingLocksProvider<MiruStreamId> streamStripingLocksProvider,
        StripingLocksProvider<String> authzStripingLocksProvider,
        PartitionErrorTracker partitionErrorTracker,
        MiruInterner<MiruTermId> termInterner,
        ObjectMapper objectMapper) {

        this.persistentCogs = persistentCogs;
        this.transientCogs = transientCogs;
        this.schemaProvider = schemaProvider;
        this.termComposer = termComposer;
        this.activityInternExtern = activityInternExtern;
        this.allocators = allocators;
        this.sipMarshaller = sipMarshaller;
        this.diskResourceLocator = diskResourceLocator;
        this.partitionAuthzCacheSize = partitionAuthzCacheSize;
        this.fieldIndexStripingLocksProvider = fieldIndexStripingLocksProvider;
        this.streamStripingLocksProvider = streamStripingLocksProvider;
        this.authzStripingLocksProvider = authzStripingLocksProvider;
        this.partitionErrorTracker = partitionErrorTracker;
        this.termInterner = termInterner;
        this.objectMapper = objectMapper;
    }

    public MiruBackingStorage findBackingStorage(MiruPartitionCoord coord) throws Exception {
        try {
            if (checkForPersistentStorage(coord)) {
                return MiruBackingStorage.disk;
            }
        } catch (MiruSchemaUnvailableException e) {
            log.warn("Schema not registered for tenant {}, using default storage", coord.tenantId);
        }
        return MiruBackingStorage.memory;
    }

    private MiruChunkAllocator getAllocator(MiruBackingStorage storage) {
        MiruChunkAllocator allocator = allocators.get(storage);
        if (allocator != null) {
            return allocator;
        } else {
            throw new RuntimeException("backingStorage:" + storage + " is unsupported.");
        }
    }

    public <BM extends IBM, IBM> MiruContext<BM, IBM, S> allocate(MiruBitmaps<BM, IBM> bitmaps,
        MiruSchema schema,
        MiruPartitionCoord coord,
        MiruBackingStorage storage,
        MiruRebuildDirector.Token rebuildToken,
        StackBuffer stackBuffer) throws Exception {

        ChunkStore[] chunkStores = getAllocator(storage).allocateChunkStores(coord, stackBuffer);
        return allocate(bitmaps, coord, schema, chunkStores, storage, rebuildToken, stackBuffer);
    }

    private <BM extends IBM, IBM> MiruContext<BM, IBM, S> allocate(MiruBitmaps<BM, IBM> bitmaps,
        MiruPartitionCoord coord,
        MiruSchema schema,
        ChunkStore[] chunkStores,
        MiruBackingStorage storage,
        MiruRebuildDirector.Token rebuildToken,
        StackBuffer stackBuffer) throws Exception {

        TxCogs cogs = storage == MiruBackingStorage.disk ? persistentCogs : transientCogs;
        int seed = new HashCodeBuilder().append(coord).append(storage).toHashCode();
        TxCog<Integer, MapBackedKeyedFPIndex, ChunkFiler> skyhookCog = cogs.getSkyhookCog(seed);
        KeyedFilerStore<Long, Void> genericFilerStore = new TxKeyedFilerStore<>(cogs, seed, chunkStores, keyBytes("generic"), false,
            TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
            TxNamedMapOfFiler.CHUNK_FILER_OPENER,
            TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
            TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER);

        MiruTimeIndex timeIndex = new MiruDeltaTimeIndex(new MiruFilerTimeIndex(
            Optional.<MiruFilerTimeIndex.TimeOrderAnomalyStream>absent(),
            new KeyedFilerProvider<>(genericFilerStore, MiruContextConstants.GENERIC_FILER_TIME_INDEX_KEY),
            new TxKeyValueStore<>(skyhookCog,
                cogs.getSkyHookKeySemaphores(),
                seed,
                chunkStores,
                new LongIntKeyValueMarshaller(),
                keyBytes("timeIndex-timestamps"),
                8, false, 4, false),
            stackBuffer));

        TxKeyedFilerStore<Long, Void> activityFilerStore = new TxKeyedFilerStore<>(cogs, seed, chunkStores, keyBytes("activityIndex"), false,
            TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
            TxNamedMapOfFiler.CHUNK_FILER_OPENER,
            TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
            TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER);

        IntUnsignedShortKeyValueMarshaller intUnsignedShortKeyValueMarshaller = new IntUnsignedShortKeyValueMarshaller();
        IntTermIdsKeyValueMarshaller intTermIdsKeyValueMarshaller = new IntTermIdsKeyValueMarshaller();

        @SuppressWarnings("unchecked")
        MiruFilerProvider<Long, Void>[] termLookup = new MiruFilerProvider[schema.fieldCount()];
        @SuppressWarnings("unchecked")
        TxKeyValueStore<Integer, MiruTermId[]>[][] termStorage = new TxKeyValueStore[16][schema.fieldCount()];
        for (MiruFieldDefinition fieldDefinition : schema.getFieldDefinitions()) {
            if (fieldDefinition.type.hasFeature(Feature.indexedValueBits)) {
                termLookup[fieldDefinition.fieldId] = new KeyedFilerProvider<>(genericFilerStore, keyBytes("termLookup-" + fieldDefinition.fieldId));

                for (int i = 0; i < 16; i++) {
                    termStorage[i][fieldDefinition.fieldId] = new TxKeyValueStore<>(skyhookCog,
                        cogs.getSkyHookKeySemaphores(),
                        seed,
                        chunkStores,
                        intTermIdsKeyValueMarshaller,
                        keyBytes("termStorage-" + i + "-" + fieldDefinition.fieldId),
                        4, false, (int) FilerIO.chunkLength(i), true);
                }
            }
        }

        MiruActivityIndex activityIndex = new MiruDeltaActivityIndex(
            new MiruFilerActivityIndex(
                activityFilerStore,
                new MiruInternalActivityMarshaller(termInterner),
                intTermIdsKeyValueMarshaller,
                new KeyedFilerProvider<>(activityFilerStore, keyBytes("activityIndex-size")),
                termLookup,
                termStorage));

        TrackError trackError = partitionErrorTracker.track(coord);

        @SuppressWarnings("unchecked")
        MiruFieldIndex<BM, IBM>[] fieldIndexes = new MiruFieldIndex[MiruFieldType.values().length];
        for (MiruFieldType fieldType : MiruFieldType.values()) {
            @SuppressWarnings("unchecked")
            KeyedFilerStore<Long, Void>[] indexes = new KeyedFilerStore[schema.fieldCount()];
            @SuppressWarnings("unchecked")
            KeyedFilerStore<Integer, MapContext>[] cardinalities = new KeyedFilerStore[schema.fieldCount()];
            for (MiruFieldDefinition fieldDefinition : schema.getFieldDefinitions()) {
                int fieldId = fieldDefinition.fieldId;
                if (fieldType == MiruFieldType.latest && !fieldDefinition.type.hasFeature(MiruFieldDefinition.Feature.indexedLatest)
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
                if (fieldDefinition.type.hasFeature(MiruFieldDefinition.Feature.cardinality)) {
                    cardinalities[fieldId] = new TxKeyedFilerStore<>(cogs,
                        seed,
                        chunkStores,
                        keyBytes("field-c-" + fieldType.name() + "-" + fieldId),
                        false,
                        new MapCreator(100, 4, false, 8, false),
                        MapOpener.INSTANCE,
                        TxMapGrower.MAP_OVERWRITE_GROWER,
                        TxMapGrower.MAP_REWRITE_GROWER);
                }
            }
            fieldIndexes[fieldType.getIndex()] = new MiruDeltaFieldIndex<>(
                bitmaps,
                trackError,
                new MiruFilerFieldIndex<>(bitmaps, trackError, indexes, cardinalities, fieldIndexStripingLocksProvider, termInterner),
                schema.getFieldDefinitions());
        }
        MiruFieldIndexProvider<BM, IBM> fieldIndexProvider = new MiruFieldIndexProvider<>(fieldIndexes);

        MiruSipIndex<S> sipIndex = new MiruDeltaSipIndex<>(new MiruFilerSipIndex<>(
            new KeyedFilerProvider<>(genericFilerStore, sipMarshaller.getSipIndexKey()),
            sipMarshaller));

        MiruAuthzUtils<BM, IBM> authzUtils = new MiruAuthzUtils<>(bitmaps);

        Cache<VersionedAuthzExpression, BM> authzCache = CacheBuilder.newBuilder()
            .maximumSize(partitionAuthzCacheSize)
            .expireAfterAccess(1, TimeUnit.MINUTES) //TODO should be adjusted with respect to tuning GC (prevent promotion from eden space)
            .build();
        MiruAuthzCache<BM, IBM> miruAuthzCache = new MiruAuthzCache<>(bitmaps, authzCache, activityInternExtern, authzUtils);

        MiruAuthzIndex<BM, IBM> authzIndex = new MiruDeltaAuthzIndex<>(bitmaps,
            trackError,
            miruAuthzCache,
            new MiruFilerAuthzIndex<>(
                bitmaps,
                trackError,
                new TxKeyedFilerStore<>(cogs, seed, chunkStores, keyBytes("authzIndex"), false,
                    TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
                    TxNamedMapOfFiler.CHUNK_FILER_OPENER,
                    TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
                    TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER),
                miruAuthzCache,
                authzStripingLocksProvider));

        MiruRemovalIndex<BM, IBM> removalIndex = new MiruDeltaRemovalIndex<>(
            bitmaps,
            trackError,
            new MiruFilerRemovalIndex<>(
                bitmaps,
                trackError,
                new TxKeyedFilerStore<>(cogs, seed, chunkStores, keyBytes("removalIndex"), false,
                    TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
                    TxNamedMapOfFiler.CHUNK_FILER_OPENER,
                    TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
                    TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER),
                new byte[] { 0 },
                new Object()),
            new MiruDeltaInvertedIndex.Delta<>());

        MiruUnreadTrackingIndex<BM, IBM> unreadTrackingIndex = new MiruDeltaUnreadTrackingIndex<>(
            bitmaps,
            trackError,
            new MiruFilerUnreadTrackingIndex<>(
                bitmaps,
                trackError,
                new TxKeyedFilerStore<>(cogs, seed, chunkStores, keyBytes("unreadTrackingIndex"), false,
                    TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
                    TxNamedMapOfFiler.CHUNK_FILER_OPENER,
                    TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
                    TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER),
                streamStripingLocksProvider));

        MiruInboxIndex<BM, IBM> inboxIndex = new MiruDeltaInboxIndex<>(
            bitmaps,
            trackError,
            new MiruFilerInboxIndex<>(
                bitmaps,
                trackError,
                new TxKeyedFilerStore<>(cogs, seed, chunkStores, keyBytes("inboxIndex"), false,
                    TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
                    TxNamedMapOfFiler.CHUNK_FILER_OPENER,
                    TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
                    TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER),
                streamStripingLocksProvider));

        StripingLocksProvider<MiruStreamId> streamLocks = new StripingLocksProvider<>(64);

        MiruContext<BM, IBM, S> context = new MiruContext<>(schema,
            termComposer,
            timeIndex,
            activityIndex,
            fieldIndexProvider,
            sipIndex,
            authzIndex,
            removalIndex,
            unreadTrackingIndex,
            inboxIndex,
            activityInternExtern,
            streamLocks,
            chunkStores,
            storage,
            rebuildToken);

        context.markStartOfDelta(stackBuffer);

        return context;
    }

    public <BM extends IBM, IBM> MiruContext<BM, IBM, S> copy(MiruBitmaps<BM, IBM> bitmaps,
        MiruSchema schema,
        MiruPartitionCoord coord,
        MiruContext<BM, IBM, S> from,
        MiruBackingStorage toStorage,
        StackBuffer stackBuffer) throws Exception {

        ChunkStore[] fromChunks = from.chunkStores;
        ChunkStore[] toChunks = getAllocator(toStorage).allocateChunkStores(coord, stackBuffer);
        if (fromChunks.length != toChunks.length) {
            throw new IllegalArgumentException("The number of from chunks:" + fromChunks.length + " must equal the number of to chunks:" + toChunks.length);
        }
        for (int i = 0; i < fromChunks.length; i++) {
            fromChunks[i].copyTo(toChunks[i], stackBuffer);
        }
        return allocate(bitmaps, coord, schema, toChunks, toStorage, null, stackBuffer);
    }

    public void saveSchema(MiruPartitionCoord coord, MiruSchema schema) throws IOException {
        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        File schemaFile = diskResourceLocator.getFilerFile(identifier, "schema");
        if (schemaFile.exists()) {
            schemaFile.delete();
        }
        objectMapper.writeValue(schemaFile, schema);
    }

    public MiruSchema loadPersistentSchema(MiruPartitionCoord coord) throws IOException {
        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        File schemaFile = diskResourceLocator.getFilerFile(identifier, "schema");
        if (schemaFile.exists()) {
            return objectMapper.readValue(schemaFile, MiruSchema.class);
        } else {
            return null;
        }
    }

    public MiruSchema lookupLatestSchema(MiruTenantId tenantId) throws MiruSchemaUnvailableException {
        return schemaProvider.getSchema(tenantId);
    }

    private boolean checkForPersistentStorage(MiruPartitionCoord coord) throws Exception {
        MiruPartitionCoordIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        File schemaFile = diskResourceLocator.getFilerFile(identifier, "schema");
        if (!schemaFile.exists()) {
            // legacy check
            File storageFile = diskResourceLocator.getFilerFile(identifier, MiruBackingStorage.disk.name());
            if (!storageFile.exists()) {
                return false;
            }
        }

        return getAllocator(MiruBackingStorage.disk).checkExists(coord);
    }

    public void markObsolete(MiruPartitionCoord coord) throws Exception {
        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        diskResourceLocator.getFilerFile(identifier, "obsolete").createNewFile();
    }

    public boolean checkObsolete(MiruPartitionCoord coord) throws Exception {
        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        return diskResourceLocator.getFilerFile(identifier, "obsolete").exists();
    }

    public void markClosed(MiruPartitionCoord coord) throws Exception {
        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        diskResourceLocator.getFilerFile(identifier, "closed").createNewFile();
    }

    public boolean checkClosed(MiruPartitionCoord coord) throws Exception {
        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        return diskResourceLocator.getFilerFile(identifier, "closed").exists();
    }

    public void cleanDisk(MiruPartitionCoord coord) throws IOException {
        diskResourceLocator.clean(new MiruPartitionCoordIdentifier(coord));
    }

    public <BM extends IBM, IBM> void close(MiruContext<BM, IBM, S> context, MiruRebuildDirector rebuildDirector) {
        if (context.rebuildToken != null) {
            rebuildDirector.release(context.rebuildToken);
        }

        context.activityIndex.close();
        context.authzIndex.close();
        context.timeIndex.close();
        context.unreadTrackingIndex.close();
        context.inboxIndex.close();

        getAllocator(context.storage).close(context.chunkStores);
    }

    public <BM extends IBM, IBM> void releaseCaches(MiruContext<BM, IBM, S> context) throws IOException {
        for (ChunkStore chunkStore : context.chunkStores) {
            chunkStore.rollCache();
        }
    }

    private byte[] keyBytes(String key) {
        return key.getBytes(Charsets.UTF_8);
    }

    private static class IntUnsignedShortKeyValueMarshaller implements KeyValueMarshaller<Integer, Integer> {

        @Override
        public byte[] keyBytes(Integer integer) {
            return FilerIO.intBytes(integer);
        }

        @Override
        public Integer bytesKey(byte[] bytes, int offset) {
            return FilerIO.bytesInt(bytes);
        }

        @Override
        public byte[] valueBytes(Integer value) {
            byte[] bytes = new byte[2];
            bytes[0] = (byte) (value >>> 8);
            bytes[1] = (byte) value.intValue();
            return bytes;
        }

        @Override
        public Integer bytesValue(Integer key, byte[] bytes, int offset) {
            int v = 0;
            v |= (bytes[offset + 0] & 0xFF);
            v <<= 8;
            v |= (bytes[offset + 1] & 0xFF);
            return v;
        }
    }
}
