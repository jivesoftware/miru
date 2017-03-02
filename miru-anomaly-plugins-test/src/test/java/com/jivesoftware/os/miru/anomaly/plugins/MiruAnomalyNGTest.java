package com.jivesoftware.os.miru.anomaly.plugins;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.bitmaps.roaring6.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.test.MiruPluginTestBootstrap;
import com.jivesoftware.os.miru.service.MiruService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author jonathan
 */
public class MiruAnomalyNGTest {

    private MiruTenantId tenant1 = new MiruTenantId("tenant1".getBytes());
    private MiruPartitionId partitionId = MiruPartitionId.of(1);

    private MiruService service;
    private AnomalyInjectable injectable;

    @BeforeMethod
    public void setUpMethod() throws Exception {
        MiruProvider<MiruService> miruProvider = new MiruPluginTestBootstrap().bootstrap(
                tenant1,
                partitionId,
                new MiruHost("logicalName"),
                new MiruSchema.Builder("test", 1)
                        .setFieldDefinitions(new MiruFieldDefinition[]{
                                new MiruFieldDefinition(0, "user", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
                                new MiruFieldDefinition(1, "doc", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
                                new MiruFieldDefinition(2, "bits", MiruFieldDefinition.Type.multiTerm, MiruFieldDefinition.Prefix.NONE)})
                        .build(),
                MiruBackingStorage.disk,
                new MiruBitmapsRoaring(),
                Collections.emptyList());

        this.service = miruProvider.getMiru(tenant1);

        this.injectable = new AnomalyInjectable(miruProvider, new Anomaly());
    }

    @Test(enabled = true)
    public void basicTest() throws Exception {
        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        int numberOfUsers = 2;
        int numDocsPerUser = 2;
        long timespan = snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(24), 0, 0);
        long intervalPerActivity = timespan / (numberOfUsers * numDocsPerUser);
        long smallestTime = snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(24), 0, 0) - timespan;
        AtomicLong time = new AtomicLong(smallestTime);

        System.out.println("Building anomalies....");
        long start = System.currentTimeMillis();
        int count = 0;
        Map<String, AtomicLong> results = new HashMap<>();
        Random r = new Random();
        for (int i = 0; i < numberOfUsers; i++) {
            String user = "bob" + i;
            for (int docId = 0; docId < numDocsPerUser; docId++) {
                AtomicLong total = results.get(user + "-" + docId);
                if (total == null) {
                    total = new AtomicLong();
                    results.put(user + "-" + docId, total);
                }
                long c = r.nextInt(10000);
                total.addAndGet(c);

                long activityTime = time.addAndGet(intervalPerActivity);
                service.writeToIndex(Collections.singletonList(viewActivity(tenant1, partitionId, activityTime, user, String.valueOf(docId), c)));
                if (++count % 10_000 == 0) {
                    System.out.println("Finished " + count + " in " + (System.currentTimeMillis() - start) + " ms");
                }
            }
        }

        System.out.println("Built and indexed " + count + " in " + (System.currentTimeMillis() - start) + "millis");

        System.out.println("Running queries...");

        long lastTime = time.get();
        final MiruTimeRange timeRange = new MiruTimeRange(smallestTime, lastTime);
        for (int i = 0; i < numberOfUsers; i++) {
            String user = "bob" + i;
            MiruFilter filter = new MiruFilter(MiruFilterOperation.or,
                    false,
                    Collections.singletonList(MiruFieldFilter.ofTerms(MiruFieldType.primary, "user", user)),
                    null);

            MiruRequest<AnomalyQuery> request = new MiruRequest<>(
                    "test",
                    tenant1,
                    MiruActorId.NOT_PROVIDED,
                    MiruAuthzExpression.NOT_PROVIDED,
                    new AnomalyQuery(
                            timeRange,
                            1,
                            "bits",
                            filter,
                            ImmutableMap.<String, MiruFilter>builder()
                                    .put(user, filter)
                                    .build(),
                            "doc",
                            Collections.singletonList("*")),
                    MiruSolutionLogLevel.INFO);
            MiruResponse<AnomalyAnswer> result = injectable.score(request);

            System.out.println("---------------------------------------");
            for (Entry<String, AnomalyAnswer.Waveform> e : result.answer.waveforms.entrySet()) {
                System.out.println("key:" + e.getKey() + " value:" + e.getValue() + " expected:" + results.get(e.getKey()));
            }
        }

    }

    @Test
    public void fundamentalTest() throws Exception {
        Random r = new Random();
        for (int t = 0; t < 1000; t++) {
            Map<String, AtomicInteger> bitCounts = new HashMap<>();
            long answer = 0;
            for (int v = 0; v < 1000; v++) {
                long value = r.nextInt(Integer.MAX_VALUE / 500);
                for (int i = 0; i < 64; i++) {
                    if (((value >> i) & 1) != 0) {

                        AtomicInteger count = bitCounts.get(String.valueOf(i));
                        if (count == null) {
                            count = new AtomicInteger();
                            bitCounts.put(String.valueOf(i), count);
                        }
                        count.incrementAndGet();
                    }
                }
                if (Integer.MAX_VALUE - answer > value) {
                    answer += value;
                } else {
                    answer = Integer.MAX_VALUE;
                }
            }

            int composed = 0;
            for (int i = 0; i < 64; i++) {
                AtomicInteger count = bitCounts.get(String.valueOf(i));
                if (count != null) {
                    long multiplier = (1L << i);
                    long add = count.get() * multiplier;

                    if (Integer.MAX_VALUE - composed > add) {
                        composed += add;
                    } else {
                        composed = Integer.MAX_VALUE;
                    }
                }
            }
            Assert.assertEquals(answer, composed);
        }

    }

    private final MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();

    public MiruPartitionedActivity viewActivity(MiruTenantId tenantId,
                                                MiruPartitionId partitionId,
                                                long time,
                                                String user,
                                                String doc,
                                                long value) {
        Map<String, List<String>> fieldsValues = Maps.newHashMap();
        fieldsValues.put("user", Collections.singletonList(user));
        fieldsValues.put("doc", Collections.singletonList(doc));

        List<String> bits = new ArrayList<>();

        for (int i = 0; i < 64; i++) {
            if (((value >> i) & 1) != 0) {
                bits.add(String.valueOf(i));
            }
        }
        fieldsValues.put("bits", bits);

        MiruActivity activity = new MiruActivity(tenantId, time, 0, false, new String[0], fieldsValues, Collections.emptyMap());
        return partitionedActivityFactory.activity(1, partitionId, 1, activity);
    }

}
