package com.jivesoftware.os.miru.service;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Interners;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.chunk.store.transaction.TxNamedMapOfFiler;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.KeyValueStore;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.filer.keyed.store.TxKeyValueStore;
import com.jivesoftware.os.filer.keyed.store.TxKeyedFilerStore;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.schema.DefaultMiruSchemaDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.schema.SingleSchemaProvider;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruTempDirectoryResourceLocator;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import com.jivesoftware.os.miru.service.stream.MiruContextFactory;
import com.jivesoftware.os.miru.service.stream.allocator.InMemoryChunkAllocator;
import com.jivesoftware.os.miru.service.stream.allocator.MiruChunkAllocator;
import com.jivesoftware.os.miru.service.stream.allocator.OnDiskChunkAllocator;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReaderImpl;
import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class IndexTestUtil {

    private static MiruContextFactory factory(int numberOfChunkStores) {
        MiruReadTrackingWALReaderImpl readTrackingWALReader = null; // TODO FActor out!

        StripingLocksProvider<MiruTermId> fieldIndexStripingLocksProvider = new StripingLocksProvider<>(1024);
        StripingLocksProvider<MiruStreamId> streamStripingLocksProvider = new StripingLocksProvider<>(1024);
        StripingLocksProvider<String> authzStripingLocksProvider = new StripingLocksProvider<>(1024);

        MiruSchemaProvider schemaProvider = new SingleSchemaProvider(
            new MiruSchema.Builder("test", 1)
            .setFieldDefinitions(DefaultMiruSchemaDefinition.FIELDS)
            .build());
        MiruTermComposer termComposer = new MiruTermComposer(Charsets.UTF_8);
        MiruActivityInternExtern activityInternExtern = new MiruActivityInternExtern(Interners.<MiruIBA>newWeakInterner(),
            Interners.<MiruTermId>newWeakInterner(),
            Interners.<MiruTenantId>newWeakInterner(),
            Interners.<String>newWeakInterner(),
            termComposer);

        MiruChunkAllocator inMemoryChunkAllocator = new InMemoryChunkAllocator(
            new HeapByteBufferFactory(),
            new HeapByteBufferFactory(),
            4_096,
            numberOfChunkStores,
            true);

        final MiruResourceLocator diskResourceLocator = new MiruTempDirectoryResourceLocator();
        MiruChunkAllocator onDiskChunkAllocator = new OnDiskChunkAllocator(diskResourceLocator,
            new HeapByteBufferFactory(),
            numberOfChunkStores);

        return new MiruContextFactory(schemaProvider,
            termComposer,
            activityInternExtern,
            readTrackingWALReader,
            ImmutableMap.<MiruBackingStorage, MiruChunkAllocator>builder()
            .put(MiruBackingStorage.memory, inMemoryChunkAllocator)
            .put(MiruBackingStorage.disk, onDiskChunkAllocator)
            .build(),
            new MiruTempDirectoryResourceLocator(),
            MiruBackingStorage.memory,
            1024,
            null,
            new AtomicLong(0),
            fieldIndexStripingLocksProvider,
            streamStripingLocksProvider,
            authzStripingLocksProvider
        );
    }

    public static <BM> MiruContext<BM> buildInMemoryContext(int numberOfChunkStores, MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord) throws Exception {
        return factory(numberOfChunkStores).allocate(bitmaps, coord, MiruBackingStorage.memory);

    }

    public static <BM> MiruContext<BM> buildOnDiskContext(int numberOfChunkStores, MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord) throws Exception {
        return factory(numberOfChunkStores).allocate(bitmaps, coord, MiruBackingStorage.disk);

    }

    public static <K, V> KeyValueStore<K, V> buildKeyValueStore(String name, ChunkStore[] chunkStores, KeyValueMarshaller<K, V> keyValueMarshaller,
        int keySize, boolean variableKeySize, int payloadSize, boolean variablePayloadSizes) {
        return new TxKeyValueStore<>(chunkStores,
            keyValueMarshaller,
            keyBytes(name),
            keySize, variableKeySize, payloadSize, variablePayloadSizes);
    }

    public static KeyedFilerStore buildKeyedFilerStore(String name, ChunkStore[] chunkStores) throws Exception {
        return new TxKeyedFilerStore(chunkStores, keyBytes(name), false,
            TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
            TxNamedMapOfFiler.CHUNK_FILER_OPENER,
            TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
            TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER);
    }

    public static ChunkStore[] buildByteBufferBackedChunkStores(int numberOfChunkStores, ByteBufferFactory byteBufferFactory, long segmentSize)
        throws Exception {

        ChunkStore[] chunkStores = new ChunkStore[numberOfChunkStores];
        ChunkStoreInitializer chunkStoreInitializer = new ChunkStoreInitializer();
        for (int i = 0; i < numberOfChunkStores; i++) {
            chunkStores[i] = chunkStoreInitializer.create(byteBufferFactory, segmentSize, new HeapByteBufferFactory(), 5_000);
        }

        return chunkStores;
    }

    public static ChunkStore[] buildFileBackedChunkStores(int numberOfChunkStores)
        throws Exception {
        File[] pathsToPartitions = new File[numberOfChunkStores];
        for (int i = 0; i < numberOfChunkStores; i++) {
            pathsToPartitions[i] = Files.createTempDirectory("chunks").toFile();
        }

        ChunkStore[] chunkStores = new ChunkStore[numberOfChunkStores];
        ChunkStoreInitializer chunkStoreInitializer = new ChunkStoreInitializer();
        for (int i = 0; i < numberOfChunkStores; i++) {
            chunkStores[i] = chunkStoreInitializer.openOrCreate(pathsToPartitions, i, "chunks-" + i, 512, new HeapByteBufferFactory(), 5_000);
        }

        return chunkStores;
    }

    private static byte[] keyBytes(String key) {
        return key.getBytes(Charsets.UTF_8);
    }

    private IndexTestUtil() {
    }
}
