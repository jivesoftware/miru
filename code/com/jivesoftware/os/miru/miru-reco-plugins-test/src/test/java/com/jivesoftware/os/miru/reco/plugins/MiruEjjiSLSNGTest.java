package com.jivesoftware.os.miru.reco.plugins;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.chunk.store.transaction.MapCreator;
import com.jivesoftware.os.filer.chunk.store.transaction.MapOpener;
import com.jivesoftware.os.filer.chunk.store.transaction.TxCogs;
import com.jivesoftware.os.filer.chunk.store.transaction.TxMapGrower;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.api.ChunkTransaction;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.filer.io.map.MapContext;
import com.jivesoftware.os.filer.io.map.MapStore;
import com.jivesoftware.os.filer.keyed.store.TxKeyedFilerStore;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

/**
 * @author jonathan
 */
public class MiruEjjiSLSNGTest {

    static TxCogs cogs = new TxCogs(256, 64, null, null, null);

    AtomicInteger walIndex = new AtomicInteger();

    int numqueries = 10_000;
    int numberOfUsers = 1_000_000;
    int numberOfDocument = 2;
    int numberOfDocumentType = 4;
    int numberOfFollows = 2_600_000;
    int numberOfStreamsPerUser = 5;
    int numberOfStreamSources = 4;
    int numberOfActivities = numberOfFollows;
    int numberOfBuckets = 32;

    @Test(enabled = false)
    public void basicTest() throws Exception {

        Random rand = new Random(1_234);
        System.out.println("Building activities....");

        File dir = Files.createTempDirectory("testNewChunkStore").toFile();
        HeapByteBufferFactory byteBufferFactory = new HeapByteBufferFactory();
        ChunkStore chunkStore1 = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data1", 8, byteBufferFactory, 500, 5_000);
        ChunkStore chunkStore2 = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data2", 8, byteBufferFactory, 500, 5_000);
        ChunkStore chunkStore3 = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data3", 8, byteBufferFactory, 500, 5_000);
        ChunkStore chunkStore4 = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data4", 8, byteBufferFactory, 500, 5_000);
        ChunkStore chunkStore5 = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data5", 8, byteBufferFactory, 500, 5_000);
        ChunkStore chunkStore6 = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data6", 8, byteBufferFactory, 500, 5_000);
        ChunkStore chunkStore7 = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data7", 8, byteBufferFactory, 500, 5_000);
        ChunkStore chunkStore8 = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data8", 8, byteBufferFactory, 500, 5_000);
        ChunkStore[] chunkStores = new ChunkStore[]{chunkStore1, chunkStore2, chunkStore3, chunkStore4,
            chunkStore5, chunkStore6, chunkStore7, chunkStore8};

        boolean ordered = false;
        TxKeyedFilerStore<Integer, MapContext> uto = new TxKeyedFilerStore<>(cogs, 0, chunkStores, "uto".getBytes(), ordered,
            new MapCreator(2, (4 + 4), false, 0, false),
            MapOpener.INSTANCE,
            TxMapGrower.MAP_OVERWRITE_GROWER,
            TxMapGrower.MAP_REWRITE_GROWER);

        TxKeyedFilerStore<Integer, MapContext> utot = new TxKeyedFilerStore<>(cogs, 0, chunkStores, "utot".getBytes(), ordered,
            new MapCreator(2, (4), false, 0, false),
            MapOpener.INSTANCE,
            TxMapGrower.MAP_OVERWRITE_GROWER,
            TxMapGrower.MAP_REWRITE_GROWER);

        TxKeyedFilerStore<Integer, MapContext> uts = new TxKeyedFilerStore<>(cogs, 0, chunkStores, "uts".getBytes(), ordered,
            new MapCreator(2, (4), false, 0, false),
            MapOpener.INSTANCE,
            TxMapGrower.MAP_OVERWRITE_GROWER,
            TxMapGrower.MAP_REWRITE_GROWER);

        TxKeyedFilerStore<Integer, MapContext> otu = new TxKeyedFilerStore<>(cogs, 0, chunkStores, "otu".getBytes(), ordered,
            new MapCreator(2, (4), false, 0, false),
            MapOpener.INSTANCE,
            TxMapGrower.MAP_OVERWRITE_GROWER,
            TxMapGrower.MAP_REWRITE_GROWER);

        TxKeyedFilerStore<Integer, MapContext> ots = new TxKeyedFilerStore<>(cogs, 0, chunkStores, "ots".getBytes(), ordered,
            new MapCreator(2, (4), false, 0, false),
            MapOpener.INSTANCE,
            TxMapGrower.MAP_OVERWRITE_GROWER,
            TxMapGrower.MAP_REWRITE_GROWER);

        TxKeyedFilerStore<Integer, MapContext> sto = new TxKeyedFilerStore<>(cogs, 0, chunkStores, "sto".getBytes(), ordered,
            new MapCreator(2, (4 + 4), false, 0, false),
            MapOpener.INSTANCE,
            TxMapGrower.MAP_OVERWRITE_GROWER,
            TxMapGrower.MAP_REWRITE_GROWER);

        TxKeyedFilerStore<Integer, MapContext> stot = new TxKeyedFilerStore<>(cogs, 0, chunkStores, "stot".getBytes(), ordered,
            new MapCreator(2, (4), false, 0, false),
            MapOpener.INSTANCE,
            TxMapGrower.MAP_OVERWRITE_GROWER,
            TxMapGrower.MAP_REWRITE_GROWER);

        ListMultimap<IBA, byte[]> buto = ArrayListMultimap.create();
        ListMultimap<IBA, byte[]> butot = ArrayListMultimap.create();
        ListMultimap<IBA, byte[]> buts = ArrayListMultimap.create();
        ListMultimap<IBA, byte[]> botu = ArrayListMultimap.create();
        ListMultimap<IBA, byte[]> bots = ArrayListMultimap.create();
        ListMultimap<IBA, byte[]> bsto = ArrayListMultimap.create();
        ListMultimap<IBA, byte[]> bstot = ArrayListMultimap.create();

        int batchSize = 100_000;
        long lastTime = System.currentTimeMillis();
        for (int i = 0; i < numberOfActivities; i++) {
            final int user = rand.nextInt(numberOfUsers);
            final int streamId = rand.nextInt(numberOfStreamsPerUser) % numberOfStreamSources;
            final int docId = rand.nextInt(numberOfDocument);
            final int docType = docId % numberOfDocumentType;

            buto.put(new IBA(FilerIO.intBytes(user)), FilerIO.intsToBytes(new int[]{docType, docId}));
            butot.put(new IBA(FilerIO.intsBytes(new int[]{user, docType})), FilerIO.intBytes(docId));
            buts.put(new IBA(FilerIO.intBytes(user)), FilerIO.intBytes(streamId));
            botu.put(new IBA(FilerIO.intsBytes(new int[]{docType, docId})), FilerIO.intBytes(user));
            bots.put(new IBA(FilerIO.intsBytes(new int[]{docType, docId})), FilerIO.intBytes(streamId));
            bsto.put(new IBA(FilerIO.intBytes(streamId)), FilerIO.intsBytes(new int[]{docType, docId}));
            bstot.put(new IBA(FilerIO.intsBytes(new int[]{streamId, docType})), FilerIO.intBytes(docId));

            if (i > 0 && i % batchSize == 0) {
                flush(buto, uto);
                flush(butot, utot);
                flush(buts, uts);
                flush(botu, otu);
                flush(bots, ots);
                flush(bsto, sto);
                flush(bstot, stot);

                long time = System.currentTimeMillis();
                System.out.println("Total:" + i + " flushed " + batchSize + " in " + (time - lastTime));
                lastTime = time;
            }
        }

        if (!buto.isEmpty()) {
            flush(buto, uto);
            flush(butot, utot);
            flush(buts, uts);
            flush(botu, otu);
            flush(bots, ots);
            flush(bsto, sto);
            flush(bstot, stot);

            long time = System.currentTimeMillis();
            System.out.println("Final flush activities " + (time - lastTime));
            lastTime = time;
        }

        System.out.println("Running queries...");

        for (int i = 0; i < numqueries; i++) {
            long s = System.currentTimeMillis();
            Integer user = rand.nextInt(numberOfUsers);
            Integer doc = rand.nextInt(numberOfDocument);
            List<Integer> docTypes = Arrays.asList(
                rand.nextInt(numberOfDocumentType),
                rand.nextInt(numberOfDocumentType),
                rand.nextInt(numberOfDocumentType));

            final AtomicLong count = new AtomicLong();
            for (Integer type : docTypes) {
                otu.read(FilerIO.intsBytes(new int[]{type, doc}), 1, new ChunkTransaction<MapContext, Void>() {

                    @Override
                    public Void commit(MapContext monkey, ChunkFiler filer, Object lock) throws IOException {
                        if (lock != null) {
                            synchronized (lock) {
                                MapStore.INSTANCE.streamKeys(filer, monkey, lock, new MapStore.KeyStream() {

                                    @Override
                                    public boolean stream(byte[] key) throws IOException {
                                        count.incrementAndGet();
                                        return true;
                                    }
                                });
                            }
                        }
                        return null;
                    }
                });
            }

            System.out.println("distinctsResult:" + count.get());
            System.out.println("Took:" + (System.currentTimeMillis() - s));

            for (File f : dir.listFiles()) {
                System.out.println("f:" + f + " size:" + f.length());
            }
        }

    }

    private void flush(ListMultimap<IBA, byte[]> buffer, TxKeyedFilerStore<Integer, MapContext> store) throws IOException {
        for (IBA k : buffer.keySet()) {
            final List<byte[]> got = buffer.get(k);
            store.readWriteAutoGrow(k.getBytes(), got.size(), new ChunkTransaction<MapContext, Void>() {

                @Override
                public Void commit(MapContext monkey, ChunkFiler f, Object lock) throws IOException {
                    if (lock != null) {
                        synchronized (lock) {
                            for (byte[] g : got) {
                                MapStore.INSTANCE.add(f, monkey, (byte) 1, g, new byte[0]);
                            }
                        }
                    }
                    return null;
                }
            });
        }
        buffer.clear();
    }

}
