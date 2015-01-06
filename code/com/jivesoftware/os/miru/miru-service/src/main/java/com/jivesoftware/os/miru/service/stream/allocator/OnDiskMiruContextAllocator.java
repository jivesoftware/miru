package com.jivesoftware.os.miru.service.stream.allocator;

import com.jivesoftware.os.filer.io.primative.LongIntKeyValueMarshaller;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.chunk.store.ChunkStore;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.keyed.store.KeyedFilerStore;
import com.jivesoftware.os.filer.keyed.store.TxKeyValueStore;
import com.jivesoftware.os.filer.keyed.store.TxKeyedFilerStore;
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
import com.jivesoftware.os.miru.service.index.disk.MiruChunkActivityIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskAuthzIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskFieldIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskInboxIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskRemovalIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskTimeIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.locator.MiruDiskResourceAnalyzer;
import com.jivesoftware.os.miru.service.locator.MiruPartitionCoordIdentifier;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruResourcePartitionIdentifier;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class OnDiskMiruContextAllocator implements MiruContextAllocator {

    private static final String DISK_FORMAT_VERSION = "version-10";

    private final String contextName;
    private final MiruSchemaProvider schemaProvider;
    private final MiruActivityInternExtern activityInternExtern;
    private final MiruReadTrackingWALReader readTrackingWALReader;
    private final MiruResourceLocator resourceLocator;
    private final int numberOfChunkStores;
    private final int partitionAuthzCacheSize;
    private final MiruDiskResourceAnalyzer diskResourceAnalyzer = new MiruDiskResourceAnalyzer();
    private final int partitionChunkStoreConcurrencyLevel;
    private final StripingLocksProvider<MiruTermId> fieldIndexStripingLocksProvider;
    private final StripingLocksProvider<MiruStreamId> streamStripingLocksProvider;
    private final StripingLocksProvider<String> authzStripingLocksProvider;

    public OnDiskMiruContextAllocator(String contextName,
        MiruSchemaProvider schemaProvider,
        MiruActivityInternExtern activityInternExtern,
        MiruReadTrackingWALReader readTrackingWALReader,
        MiruResourceLocator resourceLocator,
        int numberOfChunkStores,
        int partitionAuthzCacheSize,
        int partitionChunkStoreConcurrencyLevel,
        StripingLocksProvider<MiruTermId> fieldIndexStripingLocksProvider,
        StripingLocksProvider<MiruStreamId> streamStripingLocksProvider, StripingLocksProvider<String> authzStripingLocksProvider) {
        this.contextName = contextName;
        this.schemaProvider = schemaProvider;
        this.activityInternExtern = activityInternExtern;
        this.readTrackingWALReader = readTrackingWALReader;
        this.resourceLocator = resourceLocator;
        this.numberOfChunkStores = numberOfChunkStores;
        this.partitionAuthzCacheSize = partitionAuthzCacheSize;
        this.partitionChunkStoreConcurrencyLevel = partitionChunkStoreConcurrencyLevel;
        this.fieldIndexStripingLocksProvider = fieldIndexStripingLocksProvider;
        this.streamStripingLocksProvider = streamStripingLocksProvider;
        this.authzStripingLocksProvider = authzStripingLocksProvider;
    }

    @Override
    public boolean checkMarkedStorage(MiruPartitionCoord coord) throws Exception {
        List<String> mapDirectories = Lists.newArrayList("activity", "index", "authz", "unread", "inbox", "timestampToIndex");

        MiruPartitionCoordIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        String[] chunkPaths = filesToPaths(resourceLocator.getChunkDirectories(identifier, "chunks"));
        if (!new ChunkStoreInitializer().checkExists(chunkPaths, "chunks", numberOfChunkStores)) {
            return false;
        }

        return diskResourceAnalyzer.checkExists(
            resourceLocator.getPartitionPaths(identifier),
            Lists.newArrayList(DISK_FORMAT_VERSION),
            mapDirectories);
    }

    @Override
    public <BM> MiruContext<BM> allocate(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord) throws Exception {
        //TODO refactor OnDisk impls to take a shared VariableKeySizeFileBackedKeyedStore and a prefixed KeyProvider

        // check for schema first
        MiruSchema schema = schemaProvider.getSchema(coord.tenantId);

        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);

        File versionFile = resourceLocator.getFilerFile(identifier, DISK_FORMAT_VERSION);
        versionFile.createNewFile();

        File contextNameFile = resourceLocator.getFilerFile(identifier, contextName);
        contextNameFile.createNewFile();

        String[] chunkPaths = filesToPaths(resourceLocator.getChunkDirectories(identifier, "chunks"));
        ChunkStore[] chunkStores = new ChunkStore[numberOfChunkStores];
        ChunkStoreInitializer chunkStoreInitializer = new ChunkStoreInitializer();
        for (int i = 0; i < numberOfChunkStores; i++) {
            chunkStores[i] = chunkStoreInitializer.initialize(
                chunkPaths,
                "chunks",
                i,
                resourceLocator.getInitialChunkSize(),
                true,
                partitionChunkStoreConcurrencyLevel);
        }

        KeyedFilerStore timeIndexFilerStore = new TxKeyedFilerStore(chunkStores, keyBytes("timeIndex-index"));
        MiruOnDiskTimeIndex timeIndex = new MiruOnDiskTimeIndex(
            new KeyedFilerProvider(timeIndexFilerStore, new byte[] { 0 }),
            new TxKeyValueStore<>(chunkStores,
                new LongIntKeyValueMarshaller(),
                keyBytes("timeIndex-timestamps"),
                4, false, 8, false));

        TxKeyedFilerStore activityFilerStore = new TxKeyedFilerStore(chunkStores, keyBytes("activityIndex"));
        MiruChunkActivityIndex activityIndex = new MiruChunkActivityIndex(
            activityFilerStore,
            new MiruInternalActivityMarshaller(),
            new KeyedFilerProvider(activityFilerStore, keyBytes("activityIndex-size")));

        @SuppressWarnings("unchecked")
        MiruOnDiskFieldIndex<BM>[] fieldIndexes = new MiruOnDiskFieldIndex[MiruFieldType.values().length];
        for (MiruFieldType fieldType : MiruFieldType.values()) {
            KeyedFilerStore[] indexes = new KeyedFilerStore[schema.fieldCount()];
            for (MiruFieldDefinition fieldDefinition : schema.getFieldDefinitions()) {
                int fieldId = fieldDefinition.fieldId;
                if (fieldType == MiruFieldType.latest && !fieldDefinition.indexLatest ||
                    fieldType == MiruFieldType.pairedLatest && fieldDefinition.pairedLatestFieldNames.isEmpty() ||
                    fieldType == MiruFieldType.bloom && fieldDefinition.bloomFieldNames.isEmpty()) {
                    indexes[fieldId] = null;
                } else {
                    indexes[fieldId] = new TxKeyedFilerStore(chunkStores, keyBytes("field-" + fieldType.name() + "-" + fieldId));
                }
            }
            fieldIndexes[fieldType.getIndex()] = new MiruOnDiskFieldIndex<>(bitmaps, indexes, fieldIndexStripingLocksProvider);
        }
        MiruFieldIndexProvider<BM> fieldIndexProvider = new MiruFieldIndexProvider<>(fieldIndexes);

        MiruAuthzUtils<BM> authzUtils = new MiruAuthzUtils<>(bitmaps);

        //TODO share the cache?
        Cache<VersionedAuthzExpression, BM> authzCache = CacheBuilder.newBuilder()
            .maximumSize(partitionAuthzCacheSize)
            .expireAfterAccess(1, TimeUnit.MINUTES) //TODO should be adjusted with respect to tuning GC (prevent promotion from eden space)
            .build();
        MiruOnDiskAuthzIndex<BM> authzIndex = new MiruOnDiskAuthzIndex<>(bitmaps,
            new TxKeyedFilerStore(chunkStores, keyBytes("authzIndex")),
            new MiruAuthzCache<>(bitmaps, authzCache, activityInternExtern, authzUtils),
            authzStripingLocksProvider);

        MiruOnDiskRemovalIndex<BM> removalIndex = new MiruOnDiskRemovalIndex<>(bitmaps,
            new TxKeyedFilerStore(chunkStores, keyBytes("removalIndex")),
            new byte[] { 0 },
            -1,
            512, //TODO expose to config
            new Object());

        MiruOnDiskUnreadTrackingIndex<BM> unreadTrackingIndex = new MiruOnDiskUnreadTrackingIndex<>(bitmaps,
            new TxKeyedFilerStore(chunkStores, keyBytes("unreadTrackingIndex")),
            streamStripingLocksProvider);

        MiruOnDiskInboxIndex<BM> inboxIndex = new MiruOnDiskInboxIndex<>(bitmaps,
            new TxKeyedFilerStore(chunkStores, keyBytes("inboxIndex")));

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

    @Override
    public <BM> MiruContext<BM> stateChanged(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord, MiruContext<BM> from, MiruPartitionState state)
        throws Exception {
        return from;
    }

    @Override
    public <BM> void close(MiruContext<BM> context) {
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
