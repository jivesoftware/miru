package com.jivesoftware.os.miru.service.stream.allocator;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.keyed.store.PartitionedMapChunkBackedKeyedStore;
import com.jivesoftware.os.filer.keyed.store.RandomAccessSwappableFiler;
import com.jivesoftware.os.filer.keyed.store.VariableKeySizeMapChunkBackedKeyedStore;
import com.jivesoftware.os.filer.map.store.FileBackedMapChunkFactory;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndexProvider;
import com.jivesoftware.os.miru.plugin.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
import com.jivesoftware.os.miru.service.index.MiruInternalActivityMarshaller;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import com.jivesoftware.os.miru.service.index.auth.VersionedAuthzExpression;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskActivityIndex;
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
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class OnDiskMiruContextAllocator implements MiruContextAllocator {

    private static final String DISK_FORMAT_VERSION = "version-8";

    //TODO push to schema
    private static final int[] ON_DISK_FIELD_KEY_SIZE_THRESHOLDS = new int[] { 4, 16, 64, 256, 1_024 };

    private final String contextName;
    private final MiruSchemaProvider schemaProvider;
    private final MiruActivityInternExtern activityInternExtern;
    private final MiruReadTrackingWALReader readTrackingWALReader;
    private final MiruResourceLocator resourceLocator;
    private final MiruFilerProviderFactory filerProviderFactory;
    private final int numberOfChunkStores;
    private final int partitionAuthzCacheSize;
    private final MiruDiskResourceAnalyzer diskResourceAnalyzer = new MiruDiskResourceAnalyzer();
    private final int partitionChunkStoreConcurrencyLevel;
    private final int partitionChunkStoreStripingLevel;

    public OnDiskMiruContextAllocator(String contextName,
        MiruSchemaProvider schemaProvider,
        MiruActivityInternExtern activityInternExtern,
        MiruReadTrackingWALReader readTrackingWALReader,
        MiruResourceLocator resourceLocator,
        MiruFilerProviderFactory filerProviderFactory,
        int numberOfChunkStores,
        int partitionAuthzCacheSize,
        int partitionChunkStoreConcurrencyLevel,
        int partitionChunkStoreStripingLevel) {
        this.contextName = contextName;
        this.schemaProvider = schemaProvider;
        this.activityInternExtern = activityInternExtern;
        this.readTrackingWALReader = readTrackingWALReader;
        this.resourceLocator = resourceLocator;
        this.filerProviderFactory = filerProviderFactory;
        this.numberOfChunkStores = numberOfChunkStores;
        this.partitionAuthzCacheSize = partitionAuthzCacheSize;
        this.partitionChunkStoreConcurrencyLevel = partitionChunkStoreConcurrencyLevel;
        this.partitionChunkStoreStripingLevel = partitionChunkStoreStripingLevel;
    }

    @Override
    public boolean checkMarkedStorage(MiruPartitionCoord coord) throws Exception {
        List<String> mapDirectories = Lists.newArrayList("activity", "index", "authz", "unread", "inbox", "timestampToIndex");

        MiruPartitionCoordIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        String[] chunkPaths = filesToPaths(resourceLocator.getChunkDirectories(identifier, "chunk"));
        if (!new ChunkStoreInitializer().checkExists(chunkPaths, "stream", numberOfChunkStores)) {
            return false;
        }

        return diskResourceAnalyzer.checkExists(
            resourceLocator.getPartitionPaths(identifier),
            Lists.newArrayList(DISK_FORMAT_VERSION, "timeIndex", "activity", "removal"),
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

        MultiChunkStore multiChunkStore = new ChunkStoreInitializer().initializeMultiFileBacked(
            filesToPaths(resourceLocator.getChunkDirectories(identifier, "chunk")),
            "stream",
            numberOfChunkStores,
            resourceLocator.getInitialChunkSize(),
            true,
            partitionChunkStoreConcurrencyLevel,
            partitionChunkStoreStripingLevel);

        MiruOnDiskTimeIndex timeIndex = new MiruOnDiskTimeIndex(
            filerProviderFactory.getFilerProvider(identifier, "timeIndex"),
            filesToPaths(resourceLocator.getMapDirectories(identifier, "timestampToIndex")));

        MiruOnDiskActivityIndex activityIndex = new MiruOnDiskActivityIndex(
            new PartitionedMapChunkBackedKeyedStore(
                new FileBackedMapChunkFactory(4, false, 8, false, 100,
                    filesToPaths(resourceLocator.getMapDirectories(identifier, "activity"))),
                new FileBackedMapChunkFactory(4, false, 8, false, 100,
                    filesToPaths(resourceLocator.getSwapDirectories(identifier, "activity"))),
                multiChunkStore,
                24),
            new MiruInternalActivityMarshaller(),
            resourceLocator.getRandomAccessFiler(identifier, "activity", "rw"));

        File[] baseIndexMapDirectories = resourceLocator.getMapDirectories(identifier, "index");
        File[] baseIndexSwapDirectories = resourceLocator.getSwapDirectories(identifier, "index");

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

            fieldIndexes[fieldType.getIndex()] = new MiruOnDiskFieldIndex<>(bitmaps, indexes);
        }
        MiruFieldIndexProvider<BM> fieldIndexProvider = new MiruFieldIndexProvider<>(fieldIndexes);

        MiruAuthzUtils<BM> authzUtils = new MiruAuthzUtils<>(bitmaps);

        //TODO share the cache?
        Cache<VersionedAuthzExpression, BM> authzCache = CacheBuilder.newBuilder()
            .maximumSize(partitionAuthzCacheSize)
            .expireAfterAccess(1, TimeUnit.MINUTES) //TODO should be adjusted with respect to tuning GC (prevent promotion from eden space)
            .build();
        MiruOnDiskAuthzIndex<BM> authzIndex = new MiruOnDiskAuthzIndex<>(bitmaps,
            filesToPaths(resourceLocator.getMapDirectories(identifier, "authz")),
            filesToPaths(resourceLocator.getSwapDirectories(identifier, "authz")),
            multiChunkStore,
            new MiruAuthzCache<>(bitmaps, authzCache, activityInternExtern, authzUtils));

        MiruOnDiskRemovalIndex<BM> removalIndex = new MiruOnDiskRemovalIndex<>(bitmaps, new RandomAccessSwappableFiler(
            resourceLocator.getFilerFile(identifier, "removal")), new Object());

        MiruOnDiskUnreadTrackingIndex<BM> unreadTrackingIndex = new MiruOnDiskUnreadTrackingIndex<>(bitmaps,
            filesToPaths(resourceLocator.getMapDirectories(identifier, "unread")),
            filesToPaths(resourceLocator.getSwapDirectories(identifier, "unread")),
            multiChunkStore);

        MiruOnDiskInboxIndex<BM> inboxIndex = new MiruOnDiskInboxIndex<>(bitmaps,
            filesToPaths(resourceLocator.getMapDirectories(identifier, "inbox")),
            filesToPaths(resourceLocator.getSwapDirectories(identifier, "inbox")),
            multiChunkStore);

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
            Optional.<MultiChunkStore>absent(),
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

    private String[] filesToPaths(File[] files) {
        String[] paths = new String[files.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = files[i].getAbsolutePath();
        }
        return paths;
    }

    public static interface MiruFilerProviderFactory {
        MiruFilerProvider getFilerProvider(MiruResourcePartitionIdentifier identifier, String name);
    }

    public static class MemMappedFilerProvider implements MiruFilerProvider {

        private final MiruResourcePartitionIdentifier identifier;
        private final String name;
        private final MiruResourceLocator resourceLocator;

        public MemMappedFilerProvider(MiruResourcePartitionIdentifier identifier, String name, MiruResourceLocator resourceLocator) {
            this.identifier = identifier;
            this.name = name;
            this.resourceLocator = resourceLocator;
        }

        @Override
        public File getBackingFile() throws IOException {
            return resourceLocator.getFilerFile(identifier, name);
        }

        @Override
        public Filer getFiler(long length) throws IOException {
            return resourceLocator.getByteBufferBackedFiler(identifier, name, length);
        }
    }

    public static class OnDiskFilerProvider implements MiruFilerProvider {

        private final MiruResourcePartitionIdentifier identifier;
        private final String name;
        private final MiruResourceLocator resourceLocator;

        public OnDiskFilerProvider(MiruResourcePartitionIdentifier identifier, String name, MiruResourceLocator resourceLocator) {
            this.identifier = identifier;
            this.name = name;
            this.resourceLocator = resourceLocator;
        }

        @Override
        public File getBackingFile() throws IOException {
            return resourceLocator.getFilerFile(identifier, name);
        }

        @Override
        public Filer getFiler(long length) throws IOException {
            return resourceLocator.getRandomAccessFiler(identifier, name, "rw");
        }
    }
}
