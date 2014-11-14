package com.jivesoftware.os.miru.service.index.disk;

import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.filer.io.ByteBufferProvider;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.KeyPartitioner;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.keyed.store.AutoResizingChunkFiler;
import com.jivesoftware.os.filer.keyed.store.AutoResizingChunkSwappableFiler;
import com.jivesoftware.os.filer.map.store.ByteBufferProviderBackedMapChunkFactory;
import com.jivesoftware.os.filer.map.store.PartitionedMapChunkBackedMapStore;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsRoaring;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

public class MiruOnDiskInvertedIndexTest {

    @Test(enabled = false)
    public void testSetGetIndex() throws Exception {
        HeapByteBufferFactory byteBufferFactory = new HeapByteBufferFactory();
        final MultiChunkStore multiChunkStore = new ChunkStoreInitializer().initializeMultiByteBufferBacked("test", byteBufferFactory, 1, 512, true, 1, 1);
        ByteBufferProviderBackedMapChunkFactory mapChunkFactory = new ByteBufferProviderBackedMapChunkFactory(4, false, 8, false, 512,
            new ByteBufferProvider("test-bbp", byteBufferFactory));
        KeyPartitioner<IBA> keyPartitioner = new KeyPartitioner<IBA>() {
            @Override
            public String keyPartition(IBA key) {
                return "0";
            }

            @Override
            public Iterable<String> allPartitions() {
                return Arrays.asList("0");
            }
        };
        KeyValueMarshaller<IBA, IBA> keyValueMarshaller = new KeyValueMarshaller<IBA, IBA>() {
            @Override
            public byte[] valueBytes(IBA value) {
                return value.getBytes();
            }

            @Override
            public IBA bytesValue(IBA key, byte[] value, int valueOffset) {
                return new IBA(value);
            }

            @Override
            public byte[] keyBytes(IBA key) {
                return key.getBytes();
            }

            @Override
            public IBA bytesKey(byte[] keyBytes, int offset) {
                return new IBA(keyBytes);
            }
        };
        final PartitionedMapChunkBackedMapStore<IBA, IBA> mapStore = new PartitionedMapChunkBackedMapStore<>(
            mapChunkFactory, 8, null, keyPartitioner, keyValueMarshaller);
        final PartitionedMapChunkBackedMapStore<IBA, IBA> swapStore = new PartitionedMapChunkBackedMapStore<>(
            mapChunkFactory, 8, null, keyPartitioner, keyValueMarshaller);

        IBA key = new IBA(FilerIO.intBytes(1));
        AutoResizingChunkFiler autoResizingChunkFiler = new AutoResizingChunkFiler(mapStore, key, multiChunkStore);
        autoResizingChunkFiler.init(32);

        final MiruOnDiskInvertedIndex<RoaringBitmap> invertedIndex = new MiruOnDiskInvertedIndex<>(
            new MiruBitmapsRoaring(),
            new AutoResizingChunkSwappableFiler(autoResizingChunkFiler, multiChunkStore, key, mapStore, swapStore),
            new Object());

        final int numIterations = 1_000;
        int poolSize = 24;
        final ExecutorService executor = Executors.newFixedThreadPool(poolSize);
        int numRunnables = 24;
        List<Runnable> runnables = new ArrayList<>(numRunnables);

        for (int i = 0; i < numRunnables; i++) {
            final int _i = i;
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    int start = _i * numIterations;
                    for (int j = start; j < start + numIterations; j++) {
                        try {
                            invertedIndex.set(j);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        if ((j + 1) % 100 == 0) {
                            System.out.println("Write iteration " + _i + " -> " + (j + 1));
                        }
                    }
                }
            });
        }

        long t = System.currentTimeMillis();
        List<Future<?>> futures = new ArrayList<>(numRunnables);
        for (Runnable runnable : runnables) {
            futures.add(executor.submit(runnable));
        }
        for (Future<?> future : futures) {
            future.get();
        }
        System.out.println("Finished indexing in " + (System.currentTimeMillis() - t));

        multiChunkStore.delete();

        RoaringBitmap index = invertedIndex.getIndex();
        assertTrue(index.getCardinality() > 0);
    }
}