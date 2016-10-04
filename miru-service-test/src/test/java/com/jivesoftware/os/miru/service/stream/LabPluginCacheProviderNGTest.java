package com.jivesoftware.os.miru.service.stream;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.LABStats;
import com.jivesoftware.os.lab.LabHeapPressure;
import com.jivesoftware.os.lab.LabHeapPressure.FreeHeapStrategy;
import com.jivesoftware.os.lab.api.rawhide.FixedWidthRawhide;
import com.jivesoftware.os.lab.guts.StripingBolBufferLocks;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.LastIdCacheKeyValues;
import com.jivesoftware.os.miru.plugin.context.LastIdKeyValueRawhide;
import com.jivesoftware.os.miru.service.stream.LabPluginCacheProvider.LabPluginCacheProviderLock;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

/**
 *
 */
public class LabPluginCacheProviderNGTest {
    @Test
    public void testEverything() throws Exception {
        OrderIdProviderImpl orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1), new SnowflakeIdPacker(),
            new JiveEpochTimestampProvider());

        LABStats labStats = new LABStats(60, 0L, 100L);
        File root = Files.createTempDir().getAbsoluteFile();
        System.out.println(root);
        LABEnvironment[] labEnvironments = {
            new LABEnvironment(labStats,
                LABEnvironment.buildLABSchedulerThreadPool(2),
                LABEnvironment.buildLABCompactorThreadPool(2),
                LABEnvironment.buildLABDestroyThreadPool(2),
                "test",
                1024 * 1024 * 10L,
                1000L,
                1024 * 1024 * 10L,
                -1L,
                root,
                new LabHeapPressure(labStats,
                    LABEnvironment.buildLABHeapSchedulerThreadPool(2),
                    "test-lhp",
                    1024 * 1024 * 10L,
                    1024 * 1024 * 20L,
                    new AtomicLong(),
                    FreeHeapStrategy.mostBytesFirst),
                2,
                2,
                LABEnvironment.buildLeapsCache(1000, 8),
                new StripingBolBufferLocks(24),
                true,
                false)
        };

        int numCommits = 7;
        int numDistincts = 1024;
        Random r = new Random(); //new Random(21431231);
        int updateScoresLength = 5;

        for (int i = 0; i < labEnvironments.length; i++) {
            labEnvironments[i].register("lastIdKeyValue", new LastIdKeyValueRawhide());
            //labEnvironments[i].register("lastIdKeyValue", new FixedWidthRawhide(8, 4 * updateScoresLength));
        }

        LabPluginCacheProviderLock[] stripedLocks = new LabPluginCacheProviderLock[8];
        for (int i = 0; i < stripedLocks.length; i++) {
            stripedLocks[i] = new LabPluginCacheProviderLock();
        }

        LabPluginCacheProvider provider = new LabPluginCacheProvider(orderIdProvider, labEnvironments, stripedLocks);
        LastIdCacheKeyValues lastIdCache = provider.getLastIdKeyValues("testLastId", -1, false, 1024 * 1024 * 10L);
        byte[] cacheId = "strut-scores-m8".getBytes(StandardCharsets.UTF_8);

        StackBuffer stackBuffer = new StackBuffer();
        for (int j = 0; j < numCommits; j++) {
            int lastId = j;
            System.out.println("++++ " + j + " ++++");
            lastIdCache.put(cacheId, true, false,
                stream -> {
                    byte[] payload = new byte[4 * updateScoresLength];
                    int offset = 0;
                    for (int k = 0; k < updateScoresLength; k++) {
                        float score = r.nextFloat();
                        byte[] scoreBytes = FilerIO.floatBytes(score);
                        System.arraycopy(scoreBytes, 0, payload, offset, 4);
                        offset += 4;
                    }

                    byte[] key = FilerIO.longBytes(r.nextInt(numDistincts));
                    return stream.stream(key, payload, lastId);
                },
                stackBuffer);
            System.out.println("---- " + j + " ----");
        }
    }
}
