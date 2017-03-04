package com.jivesoftware.os.miru.stream.plugins.strut;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.chunk.store.transaction.KeyToFPCacheFactory;
import com.jivesoftware.os.filer.chunk.store.transaction.MapCreator;
import com.jivesoftware.os.filer.chunk.store.transaction.MapOpener;
import com.jivesoftware.os.filer.chunk.store.transaction.TxCogs;
import com.jivesoftware.os.filer.chunk.store.transaction.TxMapGrower;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.filer.io.map.MapContext;
import com.jivesoftware.os.filer.keyed.store.TxKeyedFilerStore;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.LABStats;
import com.jivesoftware.os.lab.LabHeapPressure;
import com.jivesoftware.os.lab.api.MemoryRawEntryFormat;
import com.jivesoftware.os.lab.api.NoOpFormatTransformerProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueIndexConfig;
import com.jivesoftware.os.lab.api.rawhide.KeyValueRawhide;
import com.jivesoftware.os.lab.guts.LABHashIndexType;
import com.jivesoftware.os.lab.guts.Leaps;
import com.jivesoftware.os.lab.guts.StripingBolBufferLocks;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.catwalk.shared.Scored;
import com.jivesoftware.os.miru.plugin.cache.LabLastIdCacheKeyValues;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.LastIdCacheKeyValues;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class StrutModelScorerNGTest {

    @Test(enabled = false, description = "No filer cache impl")
    public void testChunk() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        ByteBufferFactory byteBufferFactory = new HeapByteBufferFactory();

        ChunkStore[] chunkStores = new ChunkStore[2];
        ChunkStoreInitializer chunkStoreInitializer = new ChunkStoreInitializer();
        for (int i = 0; i < 2; i++) {
            chunkStores[i] = chunkStoreInitializer.create(byteBufferFactory,
                1024,
                byteBufferFactory,
                10,
                100,
                stackBuffer);
        }

        TxCogs transientCogs = new TxCogs(1024, 1024,
            new ConcurrentKeyToFPCacheFactory(),
            new NullKeyToFPCacheFactory(),
            new NullKeyToFPCacheFactory());

        String catwalkId = "catwalkId";
        String modelId = "modelId";

        @SuppressWarnings("unchecked")
        KeyedFilerStore<Integer, MapContext>[] cacheStores = new KeyedFilerStore[16];
        for (int i = 0; i < cacheStores.length; i++) {
            cacheStores[i] = new TxKeyedFilerStore<>(transientCogs,
                1234,
                chunkStores,
                ("cache-" + i + "-" + catwalkId).getBytes(),
                false,
                new MapCreator(2, (int) FilerIO.chunkLength(i), true, 8, false),
                MapOpener.INSTANCE,
                TxMapGrower.MAP_OVERWRITE_GROWER,
                TxMapGrower.MAP_REWRITE_GROWER);

        }

        LastIdCacheKeyValues cacheKeyValues = null; //new MiruFilerCacheKeyValues("test", cacheStores);

        assertScores(modelId, cacheKeyValues, stackBuffer);

    }

    @Test
    public void testLab() throws Exception {

        File root = Files.createTempDir();
        LABStats labStats = new LABStats();
        LabHeapPressure labHeapPressure = new LabHeapPressure(labStats, MoreExecutors.sameThreadExecutor(), "test", 1024 * 1024 * 10, 1024 * 1024 * 20,
            new AtomicLong(), LabHeapPressure.FreeHeapStrategy.mostBytesFirst);
        LRUConcurrentBAHLinkedHash<Leaps> leapCache = LABEnvironment.buildLeapsCache(1_000_000, 8);
        StripingBolBufferLocks bolBufferLocks = new StripingBolBufferLocks(2048); // TODO config

        LABEnvironment env = new LABEnvironment(labStats,
            LABEnvironment.buildLABSchedulerThreadPool(4),
            LABEnvironment.buildLABCompactorThreadPool(4),
            LABEnvironment.buildLABDestroyThreadPool(1),
            null,
            root,
            labHeapPressure,
            4,
            10,
            leapCache,
            bolBufferLocks,
            true,
            false);
        String catwalkId = "catwalkId";
        String modelId = "modelId";

        @SuppressWarnings("unchecked")
        ValueIndex<byte[]>[] stores = new ValueIndex[16];
        for (int i = 0; i < stores.length; i++) {
            stores[i] = env.open(new ValueIndexConfig("cache-" + i + "-" + catwalkId,
                4096,
                1024 * 1024,
                0,
                0,
                0,
                NoOpFormatTransformerProvider.NAME,
                KeyValueRawhide.NAME,
                MemoryRawEntryFormat.NAME,
                20,
                LABHashIndexType.cuckoo,
                2d,
                true));
        }

        LastIdCacheKeyValues cacheKeyValues = new LabLastIdCacheKeyValues("test", new OrderIdProviderImpl(new ConstantWriterIdProvider(1)), stores);

        assertScores(modelId, cacheKeyValues, new StackBuffer());

    }

    private void assertScores(String modelId, LastIdCacheKeyValues cacheKeyValues, StackBuffer stackBuffer) throws Exception {
        MiruTermId[] termIds = new MiruTermId[] {
            new MiruTermId(new byte[] { (byte) 124 }),
            new MiruTermId(new byte[] { (byte) 124, (byte) 124 }),
            new MiruTermId(new byte[] { (byte) 124, (byte) 124, (byte) 124, (byte) 124 })
        };

        StrutModelScorer.score(new String[] { modelId }, 1, termIds, new LastIdCacheKeyValues[] { cacheKeyValues }, new float[] { 1 },
            (int termIndex, float[] scores, int lastId) -> {
                System.out.println(termIndex + " " + Arrays.toString(scores) + " " + lastId);
                return true;
            }, stackBuffer);
        System.out.println("-----------");

        List<Scored> updates = Lists.newArrayList();
        for (int i = 0; i < 1; i++) {
            updates.add(new Scored(-1, new MiruTermId(new byte[] { (byte) 97, (byte) (97 + i) }), 10, 0.5f, new float[] { 0.5f }, null, -1));
        }

        StrutModelScorer.commit(modelId, cacheKeyValues, updates, stackBuffer);

        System.out.println("-----------");

        StrutModelScorer.score(new String[] { modelId }, 1, termIds, new LastIdCacheKeyValues[] { cacheKeyValues }, new float[] { 1 },
            (int termIndex, float[] scores, int lastId) -> {
                System.out.println(termIndex + " " + Arrays.toString(scores) + " " + lastId);
                return true;
            }, stackBuffer);
    }

    private static class ConcurrentKeyToFPCacheFactory implements KeyToFPCacheFactory {

        @Override
        public Map<IBA, Long> createCache() {
            return Maps.newConcurrentMap();
        }
    }

    private static class NullKeyToFPCacheFactory implements KeyToFPCacheFactory {

        @Override
        public Map<IBA, Long> createCache() {
            return null;
        }
    }

}
