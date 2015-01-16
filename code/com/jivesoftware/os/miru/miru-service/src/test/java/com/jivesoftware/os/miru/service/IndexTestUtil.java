package com.jivesoftware.os.miru.service;

import com.google.common.base.Charsets;
import com.google.common.collect.Interners;
import com.jivesoftware.os.filer.chunk.store.ChunkStore;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.keyed.store.KeyedFilerStore;
import com.jivesoftware.os.filer.keyed.store.TxKeyValueStore;
import com.jivesoftware.os.filer.keyed.store.TxKeyedFilerStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.schema.DefaultMiruSchemaDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.plugin.schema.SingleSchemaProvider;
import com.jivesoftware.os.miru.service.locator.MiruTempDirectoryResourceLocator;
import com.jivesoftware.os.miru.service.stream.allocator.HybridMiruContextAllocator;
import com.jivesoftware.os.miru.service.stream.allocator.MiruContextAllocator;
import com.jivesoftware.os.miru.service.stream.allocator.OnDiskMiruContextAllocator;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReaderImpl;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingSipWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALRow;
import com.jivesoftware.os.rcvs.inmemory.RowColumnValueStoreImpl;
import java.io.File;
import java.nio.file.Files;

/**
 *
 */
public class IndexTestUtil {

    private IndexTestUtil() {
    }

    public static MiruContextAllocator buildHybridContextAllocator(int numberOfChunkStores,
        int partitionAuthzCacheSize,
        boolean partitionDeleteChunkStoreOnClose) {

        MiruSchemaProvider schemaProvider = new SingleSchemaProvider(new MiruSchema(DefaultMiruSchemaDefinition.FIELDS));

        RowColumnValueStoreImpl<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity> readTrackingWAL =
            new RowColumnValueStoreImpl<>();
        RowColumnValueStoreImpl<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long> readTrackingSipWAL =
            new RowColumnValueStoreImpl<>();

        MiruActivityInternExtern activityInternExtern = new MiruActivityInternExtern(
            Interners.<MiruIBA>newWeakInterner(),
            Interners.<MiruTermId>newWeakInterner(),
            Interners.<MiruTenantId>newStrongInterner(),
            Interners.<String>newWeakInterner());

        MiruReadTrackingWALReaderImpl readTrackingWALReader = new MiruReadTrackingWALReaderImpl(readTrackingWAL, readTrackingSipWAL);

        return new HybridMiruContextAllocator(schemaProvider,
            activityInternExtern,
            readTrackingWALReader,
            new MiruTempDirectoryResourceLocator(),
            new HeapByteBufferFactory(),
            numberOfChunkStores,
            partitionAuthzCacheSize,
            partitionDeleteChunkStoreOnClose,
            new StripingLocksProvider<MiruTermId>(8),
            new StripingLocksProvider<MiruStreamId>(8),
            new StripingLocksProvider<String>(8));
    }

    public static MiruContextAllocator buildOnDiskContextAllocator(int numberOfChunkStores,
        int partitionAuthzCacheSize) {

        MiruSchemaProvider schemaProvider = new SingleSchemaProvider(new MiruSchema(DefaultMiruSchemaDefinition.FIELDS));

        RowColumnValueStoreImpl<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity> readTrackingWAL =
            new RowColumnValueStoreImpl<>();
        RowColumnValueStoreImpl<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long> readTrackingSipWAL =
            new RowColumnValueStoreImpl<>();

        MiruActivityInternExtern activityInternExtern = new MiruActivityInternExtern(
            Interners.<MiruIBA>newWeakInterner(),
            Interners.<MiruTermId>newWeakInterner(),
            Interners.<MiruTenantId>newStrongInterner(),
            Interners.<String>newWeakInterner());

        MiruReadTrackingWALReaderImpl readTrackingWALReader = new MiruReadTrackingWALReaderImpl(readTrackingWAL, readTrackingSipWAL);

        return new OnDiskMiruContextAllocator(schemaProvider,
            activityInternExtern,
            readTrackingWALReader,
            new MiruTempDirectoryResourceLocator(),
            numberOfChunkStores,
            partitionAuthzCacheSize,
            new StripingLocksProvider<MiruTermId>(8),
            new StripingLocksProvider<MiruStreamId>(8),
            new StripingLocksProvider<String>(8));
    }

    public static <K, V> KeyValueStore<K, V> buildKeyValueStore(String name,
        ChunkStore[] chunkStores,
        KeyValueMarshaller<K, V> keyValueMarshaller,
        int keySize,
        boolean variableKeySize,
        int payloadSize,
        boolean variablePayloadSizes) {
        return new TxKeyValueStore<>(chunkStores,
            keyValueMarshaller,
            keyBytes(name),
            keySize, variableKeySize, payloadSize, variablePayloadSizes);
    }

    public static KeyedFilerStore buildKeyedFilerStore(String name, ChunkStore[] chunkStores) throws Exception {
        return new TxKeyedFilerStore(chunkStores, keyBytes(name));
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

    public static ChunkStore[] buildFileBackedChunkStores(int numberOfChunkStores) throws Exception {
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
}
