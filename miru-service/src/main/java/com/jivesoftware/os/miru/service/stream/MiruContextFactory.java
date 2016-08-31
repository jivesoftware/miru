package com.jivesoftware.os.miru.service.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.chunk.store.transaction.MapBackedKeyedFPIndex;
import com.jivesoftware.os.filer.chunk.store.transaction.MapCreator;
import com.jivesoftware.os.filer.chunk.store.transaction.MapOpener;
import com.jivesoftware.os.filer.chunk.store.transaction.TxCog;
import com.jivesoftware.os.filer.chunk.store.transaction.TxCogs;
import com.jivesoftware.os.filer.chunk.store.transaction.TxMapGrower;
import com.jivesoftware.os.filer.chunk.store.transaction.TxNamedMapOfFiler;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.filer.io.map.MapContext;
import com.jivesoftware.os.filer.io.primative.LongIntKeyValueMarshaller;
import com.jivesoftware.os.filer.keyed.store.TxKeyValueStore;
import com.jivesoftware.os.filer.keyed.store.TxKeyedFilerStore;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.api.KeyValueRawhide;
import com.jivesoftware.os.lab.api.MemoryRawEntryFormat;
import com.jivesoftware.os.lab.api.NoOpFormatTransformerProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueIndexConfig;
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
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider;
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
import com.jivesoftware.os.miru.service.index.TimeIdIndex;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import com.jivesoftware.os.miru.service.index.auth.VersionedAuthzExpression;
import com.jivesoftware.os.miru.service.index.filer.MiruFilerActivityIndex;
import com.jivesoftware.os.miru.service.index.filer.MiruFilerAuthzIndex;
import com.jivesoftware.os.miru.service.index.filer.MiruFilerFieldIndex;
import com.jivesoftware.os.miru.service.index.filer.MiruFilerInboxIndex;
import com.jivesoftware.os.miru.service.index.filer.MiruFilerRemovalIndex;
import com.jivesoftware.os.miru.service.index.filer.MiruFilerSipIndex;
import com.jivesoftware.os.miru.service.index.filer.MiruFilerTimeIndex;
import com.jivesoftware.os.miru.service.index.filer.MiruFilerUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.index.lab.LabActivityIndex;
import com.jivesoftware.os.miru.service.index.lab.LabAuthzIndex;
import com.jivesoftware.os.miru.service.index.lab.LabFieldIndex;
import com.jivesoftware.os.miru.service.index.lab.LabInboxIndex;
import com.jivesoftware.os.miru.service.index.lab.LabRemovalIndex;
import com.jivesoftware.os.miru.service.index.lab.LabSipIndex;
import com.jivesoftware.os.miru.service.index.lab.LabTimeIndex;
import com.jivesoftware.os.miru.service.index.lab.LabUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.locator.MiruPartitionCoordIdentifier;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruResourcePartitionIdentifier;
import com.jivesoftware.os.miru.service.partition.PartitionErrorTracker;
import com.jivesoftware.os.miru.service.stream.allocator.MiruChunkAllocator;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author jonathan
 */
public class MiruContextFactory<S extends MiruSipCursor<S>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private static final int LAB_VERSION = 2;
    private static final int[] SUPPORTED_LAB_VERSIONS = { -1 };

    private static final int LAB_ATOMIZED_MIN_VERSION = 2;

    private final OrderIdProvider idProvider;
    private final TxCogs persistentCogs;
    private final TxCogs transientCogs;
    private final TimeIdIndex[] timeIdIndexes;
    private final LabPluginCacheProvider.LabPluginCacheProviderLock[] labPluginCacheProviderLocks;
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
    private final long maxHeapPressureInBytes;
    private final boolean useLabIndexes;
    private final boolean fsyncOnCommit;

    public MiruContextFactory(OrderIdProvider idProvider,
        TxCogs persistentCogs,
        TxCogs transientCogs,
        TimeIdIndex[] timeIdIndexes,
        LabPluginCacheProvider.LabPluginCacheProviderLock[] labPluginCacheProviderLocks,
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
        ObjectMapper objectMapper,
        long maxHeapPressureInBytes,
        boolean useLabIndexes,
        boolean fsyncOnCommit) {

        this.idProvider = idProvider;
        this.persistentCogs = persistentCogs;
        this.transientCogs = transientCogs;
        this.timeIdIndexes = timeIdIndexes;
        this.labPluginCacheProviderLocks = labPluginCacheProviderLocks;
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
        this.maxHeapPressureInBytes = maxHeapPressureInBytes;
        this.useLabIndexes = useLabIndexes;
        this.fsyncOnCommit = fsyncOnCommit;
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
        MiruRebuildDirector.Token rebuildToken) throws Exception {

        MiruChunkAllocator allocator = getAllocator(storage);
        // check ideal case first
        if (useLabIndexes && allocator.hasLabIndex(coord, LAB_VERSION)) {
            LABEnvironment[] labEnvironments = allocator.allocateLABEnvironments(coord, LAB_VERSION);
            return allocateLabIndex(LAB_VERSION, bitmaps, coord, schema, labEnvironments, storage, rebuildToken);
        } else if (useLabIndexes && !allocator.hasChunkStores(coord)) {
            for (int labVersion : SUPPORTED_LAB_VERSIONS) {
                if (allocator.hasLabIndex(coord, labVersion)) {
                    LABEnvironment[] labEnvironments = allocator.allocateLABEnvironments(coord, labVersion);
                    return allocateLabIndex(labVersion, bitmaps, coord, schema, labEnvironments, storage, rebuildToken);
                }
            }
            // otherwise create with latest version
            LABEnvironment[] labEnvironments = allocator.allocateLABEnvironments(coord, LAB_VERSION);
            return allocateLabIndex(LAB_VERSION, bitmaps, coord, schema, labEnvironments, storage, rebuildToken);
        } else {
            ChunkStore[] chunkStores = allocator.allocateChunkStores(coord);
            return allocateChunkStore(bitmaps, coord, schema, chunkStores, storage, rebuildToken);
        }
    }

    private <BM extends IBM, IBM> MiruContext<BM, IBM, S> allocateChunkStore(MiruBitmaps<BM, IBM> bitmaps,
        MiruPartitionCoord coord,
        MiruSchema schema,
        ChunkStore[] chunkStores,
        MiruBackingStorage storage,
        MiruRebuildDirector.Token rebuildToken) throws Exception {

        long version = getVersion(coord, storage);
        if (version == -1) {
            version = idProvider.nextId();
            saveVersion(coord, version);
        }

        TxCogs cogs = storage == MiruBackingStorage.disk ? persistentCogs : transientCogs;
        int seed = new HashCodeBuilder().append(coord).append(storage).toHashCode();
        TxCog<Integer, MapBackedKeyedFPIndex, ChunkFiler> skyhookCog = cogs.getSkyhookCog(seed);
        KeyedFilerStore<Long, Void> genericFilerStore = new TxKeyedFilerStore<>(cogs, seed, chunkStores, keyBytes("generic"), false,
            TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
            TxNamedMapOfFiler.CHUNK_FILER_OPENER,
            TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
            TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER);

        MiruTimeIndex timeIndex = new MiruFilerTimeIndex(
            new KeyedFilerProvider<>(genericFilerStore, MiruContextConstants.GENERIC_FILER_TIME_INDEX_KEY),
            new TxKeyValueStore<>(skyhookCog,
                cogs.getSkyHookKeySemaphores(),
                seed,
                chunkStores,
                new LongIntKeyValueMarshaller(),
                keyBytes("timeIndex-timestamps"),
                8, false, 4, false));

        TxKeyedFilerStore<Long, Void> activityFilerStore = new TxKeyedFilerStore<>(cogs, seed, chunkStores, keyBytes("activityIndex"), false,
            TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
            TxNamedMapOfFiler.CHUNK_FILER_OPENER,
            TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
            TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER);

        IntTermIdsKeyValueMarshaller intTermIdsKeyValueMarshaller = new IntTermIdsKeyValueMarshaller();

        @SuppressWarnings("unchecked")
        MiruFilerProvider<Long, Void>[] termLookup = new MiruFilerProvider[schema.fieldCount()];
        @SuppressWarnings("unchecked")
        TxKeyValueStore<Integer, MiruTermId[]>[][] termStorage = new TxKeyValueStore[16][schema.fieldCount()];
        for (MiruFieldDefinition fieldDefinition : schema.getFieldDefinitions()) {
            if (fieldDefinition.type.hasFeature(Feature.stored)) {
                termLookup[fieldDefinition.fieldId] = new KeyedFilerProvider<>(genericFilerStore, keyBytes("termLookup2-" + fieldDefinition.fieldId));

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

        MiruActivityIndex activityIndex = new MiruFilerActivityIndex(
            new KeyedFilerProvider<>(activityFilerStore, keyBytes("activityIndex-tav")),
            intTermIdsKeyValueMarshaller,
            new KeyedFilerProvider<>(activityFilerStore, keyBytes("activityIndex-size")),
            termLookup,
            termStorage);

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

            fieldIndexes[fieldType.getIndex()] = new MiruFilerFieldIndex<>(bitmaps,
                trackError,
                indexes,
                cardinalities,
                fieldIndexStripingLocksProvider,
                termInterner);
        }
        MiruFieldIndexProvider<BM, IBM> fieldIndexProvider = new MiruFieldIndexProvider<>(fieldIndexes);

        MiruSipIndex<S> sipIndex = new MiruFilerSipIndex<>(
            new KeyedFilerProvider<>(genericFilerStore, sipMarshaller.getSipIndexKey()),
            sipMarshaller);

        MiruAuthzUtils<BM, IBM> authzUtils = new MiruAuthzUtils<>(bitmaps);

        Cache<VersionedAuthzExpression, BM> authzCache = CacheBuilder.newBuilder()
            .maximumSize(partitionAuthzCacheSize)
            .expireAfterAccess(1, TimeUnit.MINUTES) //TODO should be adjusted with respect to tuning GC (prevent promotion from eden space)
            .build();
        MiruAuthzCache<BM, IBM> miruAuthzCache = new MiruAuthzCache<>(bitmaps, authzCache, activityInternExtern, authzUtils);

        MiruAuthzIndex<BM, IBM> authzIndex = new MiruFilerAuthzIndex<>(
            bitmaps,
            trackError,
            new TxKeyedFilerStore<>(cogs, seed, chunkStores, keyBytes("authzIndex"), false,
                TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
                TxNamedMapOfFiler.CHUNK_FILER_OPENER,
                TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
                TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER),
            miruAuthzCache,
            authzStripingLocksProvider);

        MiruRemovalIndex<BM, IBM> removalIndex = new MiruFilerRemovalIndex<>(
            bitmaps,
            trackError,
            new TxKeyedFilerStore<>(cogs, seed, chunkStores, keyBytes("removalIndex"), false,
                TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
                TxNamedMapOfFiler.CHUNK_FILER_OPENER,
                TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
                TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER),
            new byte[] { 0 },
            new Object());

        MiruUnreadTrackingIndex<BM, IBM> unreadTrackingIndex = new MiruFilerUnreadTrackingIndex<>(
            bitmaps,
            trackError,
            new TxKeyedFilerStore<>(cogs, seed, chunkStores, keyBytes("unreadTrackingIndex"), false,
                TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
                TxNamedMapOfFiler.CHUNK_FILER_OPENER,
                TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
                TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER),
            streamStripingLocksProvider);

        MiruInboxIndex<BM, IBM> inboxIndex = new MiruFilerInboxIndex<>(
            bitmaps,
            trackError,
            new TxKeyedFilerStore<>(cogs, seed, chunkStores, keyBytes("inboxIndex"), false,
                TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
                TxNamedMapOfFiler.CHUNK_FILER_OPENER,
                TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
                TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER),
            streamStripingLocksProvider);

        StripingLocksProvider<MiruStreamId> streamLocks = new StripingLocksProvider<>(64);

        MiruPluginCacheProvider cacheProvider = new FilerPluginCacheProvider(cogs, seed, chunkStores);

        MiruContext<BM, IBM, S> context = new MiruContext<>(version,
            getTimeIdIndex(version),
            schema,
            termComposer,
            timeIndex,
            activityIndex,
            fieldIndexProvider,
            sipIndex,
            authzIndex,
            removalIndex,
            unreadTrackingIndex,
            inboxIndex,
            cacheProvider,
            activityInternExtern,
            streamLocks,
            chunkStores,
            null,
            storage,
            rebuildToken,
            (executorService) -> {
            },
            () -> getAllocator(storage).close(chunkStores),
            () -> getAllocator(storage).remove(chunkStores));

        return context;
    }

    private TimeIdIndex getTimeIdIndex(long version) {
        return timeIdIndexes[Math.abs((int) hash(version) % timeIdIndexes.length)];
    }

    private final static long randMult = 0x5DEECE66DL;
    private final static long randAdd = 0xBL;
    private final static long randMask = (1L << 48) - 1;

    private static long hash(long value) {
        long x = (value * randMult + randAdd) & randMask;
        long h = Math.abs(x >>> (16));
        if (h >= 0) {
            return h;
        } else {
            return Long.MAX_VALUE;
        }
    }

    private <BM extends IBM, IBM> MiruContext<BM, IBM, S> allocateLabIndex(int labVersion,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruPartitionCoord coord,
        MiruSchema schema,
        LABEnvironment[] labEnvironments,
        MiruBackingStorage storage,
        MiruRebuildDirector.Token rebuildToken) throws Exception {

        boolean atomized = (labVersion >= LAB_ATOMIZED_MIN_VERSION);

        long version = getVersion(coord, storage);

        List<ValueIndex> commitables = Lists.newArrayList();

        // do NOT hash storage, as disk/memory require the same stripe order
        int seed = new HashCodeBuilder().append(coord).toHashCode();
        ValueIndex metaIndex = labEnvironments[Math.abs(seed % labEnvironments.length)].open(new ValueIndexConfig("meta",
            4096,
            maxHeapPressureInBytes,
            10 * 1024 * 1024,
            -1L,
            -1L,
            NoOpFormatTransformerProvider.NAME,
            KeyValueRawhide.NAME,
            MemoryRawEntryFormat.NAME));
        // metaIndex is not part of commitables; it will be committed explicitly

        ValueIndex monoTimeIndex = labEnvironments[Math.abs((seed + 1) % labEnvironments.length)].open(new ValueIndexConfig("monoTime",
            4096,
            maxHeapPressureInBytes,
            10 * 1024 * 1024,
            -1L,
            -1L,
            NoOpFormatTransformerProvider.NAME,
            "fixedWidth_12_0",
            MemoryRawEntryFormat.NAME));
        commitables.add(monoTimeIndex);
        ValueIndex rawTimeIndex = labEnvironments[Math.abs((seed + 1) % labEnvironments.length)].open(new ValueIndexConfig("rawTime",
            4096,
            maxHeapPressureInBytes,
            10 * 1024 * 1024,
            -1L,
            -1L,
            NoOpFormatTransformerProvider.NAME,
            "fixedWidth_8_4",
            MemoryRawEntryFormat.NAME));
        commitables.add(rawTimeIndex);
        MiruTimeIndex timeIndex = new LabTimeIndex(
            idProvider,
            metaIndex,
            keyBytes("timeIndex"),
            monoTimeIndex,
            rawTimeIndex);

        IntTermIdsKeyValueMarshaller intTermIdsKeyValueMarshaller = new IntTermIdsKeyValueMarshaller();

        ValueIndex[] termStorage = new ValueIndex[labEnvironments.length];
        for (int i = 0; i < termStorage.length; i++) {
            termStorage[i] = labEnvironments[i].open(new ValueIndexConfig("termStorage",
                4096,
                maxHeapPressureInBytes,
                10 * 1024 * 1024,
                -1L,
                -1L,
                NoOpFormatTransformerProvider.NAME,
                KeyValueRawhide.NAME,
                MemoryRawEntryFormat.NAME));
            commitables.add(termStorage[i]);
        }
        boolean[] hasTermStorage = new boolean[schema.fieldCount()];
        for (MiruFieldDefinition fieldDefinition : schema.getFieldDefinitions()) {
            hasTermStorage[fieldDefinition.fieldId] = fieldDefinition.type.hasFeature(Feature.stored);
        }

        ValueIndex timeAndVersionIndex = labEnvironments[Math.abs((seed + 2) % labEnvironments.length)].open(new ValueIndexConfig("timeAndVersion",
            4096,
            maxHeapPressureInBytes,
            10 * 1024 * 1024,
            -1L,
            -1L,
            NoOpFormatTransformerProvider.NAME,
            "fixedWidth_4_16",
            MemoryRawEntryFormat.NAME));
        commitables.add(timeAndVersionIndex);
        MiruActivityIndex activityIndex = new LabActivityIndex(
            idProvider,
            timeAndVersionIndex,
            intTermIdsKeyValueMarshaller,
            metaIndex,
            keyBytes("lastId"),
            termStorage,
            hasTermStorage);

        TrackError trackError = partitionErrorTracker.track(coord);

        @SuppressWarnings("unchecked")
        ValueIndex[] bitmapIndex = new ValueIndex[labEnvironments.length];
        ValueIndex[] termIndex = new ValueIndex[labEnvironments.length];
        ValueIndex[] cardinalityIndex = new ValueIndex[labEnvironments.length];
        for (int i = 0; i < bitmapIndex.length; i++) {
            bitmapIndex[i] = labEnvironments[i].open(new ValueIndexConfig("field",
                4096,
                maxHeapPressureInBytes,
                10 * 1024 * 1024,
                -1L,
                -1L,
                NoOpFormatTransformerProvider.NAME,
                KeyValueRawhide.NAME,
                MemoryRawEntryFormat.NAME));
            commitables.add(bitmapIndex[i]);
            termIndex[i] = labEnvironments[i].open(new ValueIndexConfig("term",
                4096,
                maxHeapPressureInBytes,
                10 * 1024 * 1024,
                -1L,
                -1L,
                NoOpFormatTransformerProvider.NAME,
                KeyValueRawhide.NAME,
                MemoryRawEntryFormat.NAME));
            commitables.add(termIndex[i]);
            cardinalityIndex[i] = labEnvironments[i].open(new ValueIndexConfig("cardinality",
                4096,
                maxHeapPressureInBytes,
                10 * 1024 * 1024,
                -1L,
                -1L,
                NoOpFormatTransformerProvider.NAME,
                KeyValueRawhide.NAME,
                MemoryRawEntryFormat.NAME));
            commitables.add(cardinalityIndex[i]);
        }

        @SuppressWarnings("unchecked")
        MiruFieldIndex<BM, IBM>[] fieldIndexes = new MiruFieldIndex[MiruFieldType.values().length];
        for (MiruFieldType fieldType : MiruFieldType.values()) {
            boolean[] hasCardinalities = new boolean[schema.fieldCount()];
            for (MiruFieldDefinition fieldDefinition : schema.getFieldDefinitions()) {
                hasCardinalities[fieldDefinition.fieldId] = fieldDefinition.type.hasFeature(MiruFieldDefinition.Feature.cardinality);
            }

            byte[] prefix = { (byte) fieldType.getIndex() };
            fieldIndexes[fieldType.getIndex()] = new LabFieldIndex<>(idProvider,
                bitmaps,
                trackError,
                prefix,
                atomized,
                bitmapIndex,
                termIndex,
                cardinalityIndex,
                hasCardinalities,
                fieldIndexStripingLocksProvider,
                termInterner);
        }
        MiruFieldIndexProvider<BM, IBM> fieldIndexProvider = new MiruFieldIndexProvider<>(fieldIndexes);

        MiruSipIndex<S> sipIndex = new LabSipIndex<>(
            idProvider,
            metaIndex,
            keyBytes("sip"),
            keyBytes("realtimeDeliveryId"),
            sipMarshaller);

        MiruRemovalIndex<BM, IBM> removalIndex = new LabRemovalIndex<>(
            idProvider,
            bitmaps,
            trackError,
            atomized,
            metaIndex,
            keyBytes("removal"),
            new Object());

        MiruUnreadTrackingIndex<BM, IBM> unreadTrackingIndex = new LabUnreadTrackingIndex<>(
            idProvider,
            bitmaps,
            trackError,
            new byte[] { (byte) -1 },
            atomized,
            bitmapIndex,
            streamStripingLocksProvider);

        MiruInboxIndex<BM, IBM> inboxIndex = new LabInboxIndex<>(
            idProvider,
            bitmaps,
            trackError,
            new byte[] { (byte) -2 },
            atomized,
            bitmapIndex,
            streamStripingLocksProvider);

        MiruAuthzUtils<BM, IBM> authzUtils = new MiruAuthzUtils<>(bitmaps);

        Cache<VersionedAuthzExpression, BM> authzCache = CacheBuilder.newBuilder()
            .maximumSize(partitionAuthzCacheSize)
            .expireAfterAccess(1, TimeUnit.MINUTES) //TODO should be adjusted with respect to tuning GC (prevent promotion from eden space)
            .build();
        MiruAuthzCache<BM, IBM> miruAuthzCache = new MiruAuthzCache<>(bitmaps, authzCache, activityInternExtern, authzUtils);

        MiruAuthzIndex<BM, IBM> authzIndex = new LabAuthzIndex<>(
            idProvider,
            bitmaps,
            trackError,
            new byte[] { (byte) -3 },
            atomized,
            bitmapIndex,
            miruAuthzCache,
            authzStripingLocksProvider);

        StripingLocksProvider<MiruStreamId> streamLocks = new StripingLocksProvider<>(64);

        LabPluginCacheProvider cacheProvider = new LabPluginCacheProvider(idProvider, labEnvironments, labPluginCacheProviderLocks);

        MiruContext<BM, IBM, S> context = new MiruContext<>(version,
            getTimeIdIndex(version),
            schema,
            termComposer,
            timeIndex,
            activityIndex,
            fieldIndexProvider,
            sipIndex,
            authzIndex,
            removalIndex,
            unreadTrackingIndex,
            inboxIndex,
            cacheProvider,
            activityInternExtern,
            streamLocks,
            null,
            labEnvironments,
            storage,
            rebuildToken,
            (executorService) -> {
                for (ValueIndex valueIndex : commitables) {
                    valueIndex.commit(fsyncOnCommit, true);
                }

                // merge the sip index only after the other indexes are written, otherwise we risk advancing the sip cursor for a partially failed merge
                sipIndex.merge();
                metaIndex.commit(fsyncOnCommit, true);

                cacheProvider.commit(fsyncOnCommit);
            },
            () -> {
                for (ValueIndex valueIndex : commitables) {
                    valueIndex.close(true, fsyncOnCommit);
                }

                // merge the sip index only after the other indexes are written, otherwise we risk advancing the sip cursor for a partially failed merge
                sipIndex.merge();
                metaIndex.close(true, fsyncOnCommit);

                cacheProvider.close(true, fsyncOnCommit);
                getAllocator(storage).close(labEnvironments);
            },
            () -> getAllocator(storage).remove(labEnvironments));

        return context;
    }

    public <BM extends IBM, IBM> MiruContext<BM, IBM, S> copy(MiruBitmaps<BM, IBM> bitmaps,
        MiruSchema schema,
        MiruPartitionCoord coord,
        MiruContext<BM, IBM, S> from,
        MiruBackingStorage toStorage,
        StackBuffer stackBuffer) throws Exception {

        saveSchema(coord, from.schema);
        saveVersion(coord, from.version);

        if (from.chunkStores != null) {
            return copyChunkStore(bitmaps, schema, coord, from, toStorage, stackBuffer);
        } else {
            return copyLabIndex(bitmaps, schema, coord, from, toStorage, stackBuffer);
        }
    }

    public <BM extends IBM, IBM> MiruContext<BM, IBM, S> copyLabIndex(MiruBitmaps<BM, IBM> bitmaps,
        MiruSchema schema,
        MiruPartitionCoord coord,
        MiruContext<BM, IBM, S> from,
        MiruBackingStorage toStorage,
        StackBuffer stackBuffer) throws Exception {

        File[] fromLabDirs = getAllocator(from.storage).getLabDirs(coord, LAB_VERSION);
        File[] toLabDirs = getAllocator(toStorage).getLabDirs(coord, LAB_VERSION);

        if (fromLabDirs.length != toLabDirs.length) {
            throw new IllegalArgumentException("The number of from env:" + fromLabDirs.length
                + " must equal the number of to env:" + toLabDirs.length);
        }
        for (int i = 0; i < fromLabDirs.length; i++) {
            FileUtils.deleteDirectory(toLabDirs[i]);
            if (fromLabDirs[i].exists()) {
                FileUtils.forceMkdir(toLabDirs[i].getParentFile());
                Files.move(fromLabDirs[i].toPath(), toLabDirs[i].toPath(), StandardCopyOption.ATOMIC_MOVE);
            }
        }
        LABEnvironment[] environments = getAllocator(toStorage).allocateLABEnvironments(coord, LAB_VERSION);
        return allocateLabIndex(LAB_VERSION, bitmaps, coord, schema, environments, toStorage, null);
    }

    public <BM extends IBM, IBM> MiruContext<BM, IBM, S> copyChunkStore(MiruBitmaps<BM, IBM> bitmaps,
        MiruSchema schema,
        MiruPartitionCoord coord,
        MiruContext<BM, IBM, S> from,
        MiruBackingStorage toStorage,
        StackBuffer stackBuffer) throws Exception {

        ChunkStore[] fromChunks = from.chunkStores;
        ChunkStore[] toChunks = getAllocator(toStorage).allocateChunkStores(coord);
        if (fromChunks.length != toChunks.length) {
            throw new IllegalArgumentException("The number of from chunks:" + fromChunks.length + " must equal the number of to chunks:" + toChunks.length);
        }
        for (int i = 0; i < fromChunks.length; i++) {
            fromChunks[i].copyTo(toChunks[i], stackBuffer);
        }

        return allocateChunkStore(bitmaps, coord, schema, toChunks, toStorage, null);
    }

    public long getVersion(MiruPartitionCoord coord, MiruBackingStorage storage) throws IOException {
        if (storage == MiruBackingStorage.memory) {
            return idProvider.nextId();
        } else {
            MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
            File versionFile = diskResourceLocator.getFilerFile(identifier, "version");
            if (versionFile.exists()) {
                return objectMapper.readValue(versionFile, Long.class);
            }

            long version = idProvider.nextId();
            saveVersion(coord, version);
            return version;
        }
    }

    public void saveVersion(MiruPartitionCoord coord, long version) throws IOException {
        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        File versionFile = diskResourceLocator.getFilerFile(identifier, "version");
        if (versionFile.exists()) {
            versionFile.delete();
        }
        objectMapper.writeValue(versionFile, version);
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

        return getAllocator(MiruBackingStorage.disk).checkExists(coord, LAB_VERSION, SUPPORTED_LAB_VERSIONS);
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

    public <BM extends IBM, IBM> void close(MiruContext<BM, IBM, S> context) throws Exception {
        context.activityIndex.close();
        context.authzIndex.close();
        context.timeIndex.close();
        context.unreadTrackingIndex.close();
        context.inboxIndex.close();
        context.closeable.close();
    }

    public <BM extends IBM, IBM> void remove(MiruContext<BM, IBM, S> context) throws Exception {
        context.removable.remove();
    }

    public <BM extends IBM, IBM> void releaseCaches(MiruContext<BM, IBM, S> context) throws IOException {
        if (context.chunkStores != null) {
            for (ChunkStore chunkStore : context.chunkStores) {
                chunkStore.rollCache();
            }
        }
    }

    private byte[] keyBytes(String key) {
        return key.getBytes(Charsets.UTF_8);
    }

}
