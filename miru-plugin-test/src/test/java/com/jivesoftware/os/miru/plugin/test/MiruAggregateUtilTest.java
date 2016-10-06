package com.jivesoftware.os.miru.plugin.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.LABStats;
import com.jivesoftware.os.lab.LabHeapPressure;
import com.jivesoftware.os.lab.api.MemoryRawEntryFormat;
import com.jivesoftware.os.lab.api.NoOpFormatTransformerProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueIndexConfig;
import com.jivesoftware.os.lab.api.rawhide.LABRawhide;
import com.jivesoftware.os.lab.guts.Leaps;
import com.jivesoftware.os.lab.guts.StripingBolBufferLocks;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.bitmaps.roaring5.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.cache.LabTimestampedCacheKeyValues;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil.GetAllTermIds;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

/**
 *
 */
public class MiruAggregateUtilTest {

    @Test
    public void testGatherFeatures() throws Exception {
        MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(0), new MiruHost("localhost"));
        MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        StackBuffer stackBuffer = new StackBuffer();
        OrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1), new SnowflakeIdPacker(), new JiveEpochTimestampProvider());

        File root = Files.createTempDir();
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        LABStats labStats = new LABStats();
        LabHeapPressure labHeapPressure = new LabHeapPressure(labStats,
            MoreExecutors.sameThreadExecutor(), "test", 1024 * 1024 * 10, 1024 * 1024 * 20, new AtomicLong(), LabHeapPressure.FreeHeapStrategy.mostBytesFirst);
        StripingBolBufferLocks bolBufferLocks = new StripingBolBufferLocks(2048); // TODO config
        LABEnvironment env = new LABEnvironment(labStats,
            LABEnvironment.buildLABSchedulerThreadPool(4),
            LABEnvironment.buildLABCompactorThreadPool(4),
            LABEnvironment.buildLABDestroyThreadPool(1),
            "wal",
            "labMeta",
            -1,
            -1,
            -1,
            -1,
            root,
            labHeapPressure,
            4,
            10,
            leapsCache,
            bolBufferLocks,
            true,
            false);

        LabTimestampedCacheKeyValues cacheKeyValues = new LabTimestampedCacheKeyValues("lab-test",
            orderIdProvider,
            new ValueIndex[]{
                env.open(new ValueIndexConfig("primary",
                    4096,
                    10 * 1024 * 1024,
                    -1,
                    -1,
                    -1,
                    NoOpFormatTransformerProvider.NAME,
                    LABRawhide.NAME,
                    MemoryRawEntryFormat.NAME,
                    20))
            },
            new Object[]{new Object(), new Object(), new Object(), new Object()});

        int fieldCount = 3;
        List<Map<Integer, String>> activityFieldTerms = Lists.newArrayList(
            ImmutableMap.of(0, "bob",
                1, "creates",
                2, "doc1"),
            ImmutableMap.of(0, "bob",
                1, "views",
                2, "doc1"),
            ImmutableMap.of(0, "jane",
                1, "views",
                2, "doc1"),
            ImmutableMap.of(0, "jane",
                1, "likes",
                2, "doc1"),
            ImmutableMap.of(0, "bob",
                1, "creates",
                2, "doc2"),
            ImmutableMap.of(0, "bob",
                1, "likes",
                2, "doc2"));

        GetAllTermIds getAllTermIds = (name, ids, offset, count, fieldId, stackBuffer1) -> {
            MiruTermId[][] termIds = new MiruTermId[count][];
            for (int i = 0; i < count; i++) {
                int index = ids[offset + i];
                String term = activityFieldTerms.get(index).get(fieldId);
                termIds[i] = term == null ? new MiruTermId[0] : new MiruTermId[]{new MiruTermId(term.getBytes())};
            }
            return termIds;
        };

        RoaringBitmap[] answers = {
            RoaringBitmap.bitmapOf(0, 1, 2, 3, 4, 5),
            RoaringBitmap.bitmapOf(0, 1, 2, 3, 4, 5),
            RoaringBitmap.bitmapOf(0, 1, 2, 3, 4, 5),};

        for (int toId : new int[]{0, 1, 2, 3, 4, 5}) {
            System.out.println("-------------------------------- toId: " + toId + " --------------------------------");
            aggregateUtil.gatherFeatures("test",
                coord,
                bitmaps,
                getAllTermIds,
                fieldCount,
                cacheKeyValues,
                streamBitmaps -> streamBitmaps.stream(-1, -1, 0, new MiruTermId("parent1".getBytes()), toId, answers),
                new int[][]{
                    {0, 1},
                    {0, 2},
                    {1, 2},},
                100,
                (streamIndex, lastId, answerFieldId, answerTermId, answerScoredLastId, featureId, termIds, count) -> {
                    log(streamIndex, lastId, answerFieldId, answerTermId, answerScoredLastId, featureId, termIds, count);
                    return true;
                },
                new MiruSolutionLog(MiruSolutionLogLevel.NONE),
                stackBuffer);
        }
    }

    private void log(int streamIndex,
        int lastId,
        int answerFieldId,
        MiruTermId answerTermId,
        int answerScoredLastId,
        int featureId,
        MiruTermId[] termIds,
        int count) {
        System.out.println(
            "streamIndex:" + streamIndex
            + ", lastId:" + lastId
            + ", answerFieldId:" + answerFieldId
            + ", answerTermId:" + answerTermId
            + ", answerScoredLastId:" + answerScoredLastId
            + ", featureId:" + featureId
            + ", termIds:" + Arrays.toString(termIds)
            + ", count1:" + count);
    }
}
