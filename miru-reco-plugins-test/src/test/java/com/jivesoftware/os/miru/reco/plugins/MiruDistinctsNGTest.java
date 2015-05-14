/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.miru.reco.plugins;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.solution.JacksonMiruSolutionMarshaller;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.test.MiruPluginTestBootstrap;
import com.jivesoftware.os.miru.reco.plugins.distincts.Distincts;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsAnswer;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsInjectable;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsReport;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsRoaring;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

/**
 * @author jonathan
 */
public class MiruDistinctsNGTest {

    MiruSchema miruSchema = new MiruSchema.Builder("test", 1)
        .setFieldDefinitions(new MiruFieldDefinition[] {
            new MiruFieldDefinition(0, "user", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
            new MiruFieldDefinition(1, "doc", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
            new MiruFieldDefinition(2, "obj", MiruFieldDefinition.Type.multiTerm,
                new MiruFieldDefinition.Prefix(MiruFieldDefinition.Prefix.Type.numeric, 4, ' ')),
            new MiruFieldDefinition(3, "text", MiruFieldDefinition.Type.multiTerm, MiruFieldDefinition.Prefix.WILDCARD)
        })
        .build();

    MiruTenantId tenant1 = new MiruTenantId("tenant1".getBytes());
    MiruPartitionId partitionId = MiruPartitionId.of(1);
    MiruHost miruHost = new MiruHost("logicalName", 1_234);
    CollaborativeFilterUtil util = new CollaborativeFilterUtil();
    AtomicInteger walIndex = new AtomicInteger();

    int numqueries = 2;
    int numberOfUsers = 2;
    int numberOfDocument = 100;
    int numberOfViewsPerUser = 2;
    int numberOfActivities = numberOfUsers * numberOfViewsPerUser + 18;
    int numberOfBuckets = 32;

    MiruService service;
    DistinctsInjectable injectable;

    @BeforeMethod
    public void setUpMethod() throws Exception {
        MiruProvider<MiruService> miruProvider = new MiruPluginTestBootstrap().bootstrap(tenant1, partitionId, miruHost,
            miruSchema, MiruBackingStorage.memory, new MiruBitmapsRoaring(), Collections.<MiruPartitionedActivity>emptyList());

        this.service = miruProvider.getMiru(tenant1);

        ObjectMapper mapper = new ObjectMapper();
        JacksonMiruSolutionMarshaller<DistinctsQuery, DistinctsAnswer, DistinctsReport> distinctsMarshaller = new JacksonMiruSolutionMarshaller<>(mapper,
            DistinctsQuery.class, DistinctsAnswer.class, DistinctsReport.class);

        this.injectable = new DistinctsInjectable(miruProvider, new Distincts(miruProvider.getTermComposer()), distinctsMarshaller);
    }

    @Test(enabled = true)
    public void basicTest() throws Exception {

        Random rand = new Random(1_234);
        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        long timespan = numberOfBuckets * snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(3), 0, 0);
        long intervalPerActivity = timespan / numberOfActivities;
        AtomicLong time = new AtomicLong(snowflakeIdPacker.pack(System.currentTimeMillis(), 0, 0) - timespan);
        System.out.println("Building activities....");

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "1",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "2",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "2",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "3",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "3",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "4",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "4",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "5",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "9",
            walIndex.incrementAndGet())));

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "1",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "2",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "2",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "3",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "3",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "4",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "4",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "5",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "10",
            walIndex.incrementAndGet())));

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "2",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "3",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "3",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "4",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "4",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "5",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "11",
            walIndex.incrementAndGet())));

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz", "3",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz", "4",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz", "4",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz", "5",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz", "12",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz", "12",
            walIndex.incrementAndGet())));

        System.out.println("Running queries...");

        long lastTime = time.get();
        final MiruTimeRange timeRange = new MiruTimeRange(lastTime - timespan, lastTime);

        for (MiruFieldDefinition fieldDefinition : miruSchema.getFieldDefinitions()) {
            long s = System.currentTimeMillis();
            MiruRequest<DistinctsQuery> request = new MiruRequest<>(tenant1,
                new MiruActorId(new Id(1)),
                MiruAuthzExpression.NOT_PROVIDED,
                new DistinctsQuery(timeRange, fieldDefinition.name, MiruFilter.NO_FILTER, null),
                MiruSolutionLogLevel.INFO);
            MiruResponse<DistinctsAnswer> distinctsResult = injectable.gatherDistincts(request);

            System.out.println("distinctsResult:" + distinctsResult);
            System.out.println("Took:" + (System.currentTimeMillis() - s));
        }

    }

    @Test(enabled = true)
    public void rangeTest() throws Exception {

        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        long timespan = numberOfBuckets * snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(3), 0, 0);
        long intervalPerActivity = timespan / numberOfActivities;
        AtomicLong time = new AtomicLong(snowflakeIdPacker.pack(System.currentTimeMillis(), 0, 0) - timespan);
        System.out.println("Building activities....");

        for (int i = 0; i < 100; i++) {
            service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob",
                String.valueOf(i + 1), walIndex.incrementAndGet())));
        }

        System.out.println("Running queries...");

        long lastTime = time.get();
        final MiruTimeRange timeRange = new MiruTimeRange(lastTime - timespan, lastTime);
        MiruFieldDefinition fieldDefinition = miruSchema.getFieldDefinition(miruSchema.getFieldId("obj"));

        long s = System.currentTimeMillis();
        Set<String> types = Sets.newHashSet("0", "1", "2", "3", "8", "-1");
        MiruRequest<DistinctsQuery> request = new MiruRequest<>(tenant1,
            new MiruActorId(new Id(1)),
            MiruAuthzExpression.NOT_PROVIDED,
            new DistinctsQuery(timeRange, fieldDefinition.name, MiruFilter.NO_FILTER, Lists.newArrayList(types)),
            MiruSolutionLogLevel.INFO);
        MiruResponse<DistinctsAnswer> distinctsResult = injectable.gatherDistincts(request);
        for (String result : distinctsResult.answer.results) {
            //System.out.println("Got " + result);
            String type = result.substring(0, result.indexOf(' '));
            assertTrue(types.contains(type), "Unexpected type " + type);
        }

        System.out.println("distinctsResult:" + distinctsResult);
        System.out.println("count:" + distinctsResult.answer.results.size());
        System.out.println("Took:" + (System.currentTimeMillis() - s));
    }

    @Test(enabled = true)
    public void wildcardTest() throws Exception {

        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        long timespan = numberOfBuckets * snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(3), 0, 0);
        long intervalPerActivity = timespan / numberOfActivities;
        AtomicLong time = new AtomicLong(snowflakeIdPacker.pack(System.currentTimeMillis(), 0, 0) - timespan);
        System.out.println("Building activities....");

        for (int i = 0; i < 100; i++) {
            service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob",
                String.valueOf(i + 1), walIndex.incrementAndGet())));
        }

        System.out.println("Running queries...");

        long lastTime = time.get();
        final MiruTimeRange timeRange = new MiruTimeRange(lastTime - timespan, lastTime);
        MiruFieldDefinition fieldDefinition = miruSchema.getFieldDefinition(miruSchema.getFieldId("text"));

        long s = System.currentTimeMillis();
        Set<String> wildcards = Sets.newHashSet("ca", "do", "el", "mo");
        MiruRequest<DistinctsQuery> request = new MiruRequest<>(tenant1,
            new MiruActorId(new Id(1)),
            MiruAuthzExpression.NOT_PROVIDED,
            new DistinctsQuery(timeRange, fieldDefinition.name, MiruFilter.NO_FILTER, Lists.newArrayList(wildcards)),
            MiruSolutionLogLevel.INFO);
        MiruResponse<DistinctsAnswer> distinctsResult = injectable.gatherDistincts(request);
        for (String result : distinctsResult.answer.results) {
            //System.out.println("Got " + result);
            String wildcard = result.substring(0, 2);
            assertTrue(wildcards.contains(wildcard), "Unexpected wildcard " + wildcard);
        }

        System.out.println("distinctsResult:" + distinctsResult);
        System.out.println("count:" + distinctsResult.answer.results.size());
        System.out.println("Took:" + (System.currentTimeMillis() - s));
    }

}
