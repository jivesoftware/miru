package com.jivesoftware.os.miru.reco.plugins;

import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.bitmaps.roaring5.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.test.MiruPluginTestBootstrap;
import com.jivesoftware.os.miru.reco.plugins.uniques.UniquesAnswer;
import com.jivesoftware.os.miru.reco.plugins.uniques.UniquesInjectable;
import com.jivesoftware.os.miru.reco.plugins.uniques.UniquesQuery;
import com.jivesoftware.os.miru.service.MiruService;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MiruUniquesNGTest {

    private MiruSchema miruSchema = new MiruSchema.Builder("test", 1)
            .setFieldDefinitions(new MiruFieldDefinition[]{
                    new MiruFieldDefinition(0, "user", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
                    new MiruFieldDefinition(1, "context", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
                    new MiruFieldDefinition(2, "locale", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE)
            })
            .build();

    private MiruTenantId tenant1 = new MiruTenantId("tenant1".getBytes());
    private MiruPartitionId partitionId = MiruPartitionId.of(1);

    private CollaborativeFilterUtil util = new CollaborativeFilterUtil();
    private AtomicInteger walIndex = new AtomicInteger();

    private int numberOfUsers = 10;
    private int numberOfGroups = numberOfUsers;
    private int numberOfOrgs = numberOfGroups;
    private int numberOfBuckets = 32;

    private MiruService service;
    private UniquesInjectable injectable;

    @BeforeMethod
    public void setUpMethod() throws Exception {
        MiruProvider<MiruService> miruProvider = new MiruPluginTestBootstrap().bootstrap(
                tenant1,
                partitionId,
                new MiruHost("logicalName"),
                miruSchema,
                MiruBackingStorage.disk,
                new MiruBitmapsRoaring(),
                Collections.emptyList());

        this.service = miruProvider.getMiru(tenant1);

        this.injectable = new UniquesInjectable(miruProvider);
    }

    @Test
    public void basicTest() throws Exception {
        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        long timespan = numberOfBuckets * snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(3), 0, 0);
        long intervalPerActivity = timespan / (numberOfUsers * numberOfGroups * numberOfOrgs);
        AtomicLong time = new AtomicLong(snowflakeIdPacker.pack(System.currentTimeMillis(), 0, 0) - timespan);

        System.out.println("Building uniques activities...");

        for (int i = 0; i < numberOfUsers; i++) {
            String user = "user" + i;
            for (int j = 0; j < numberOfGroups; j++) {
                String group = "group" + j;
                for (int k = 0; k < numberOfOrgs; k++) {
                    String org = "org" + k;
                    service.writeToIndex(Collections.singletonList(
                            util.globalActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity),
                                    user, group, org,
                                    walIndex.incrementAndGet())));
                }
            }
        }

        System.out.println("Running uniques queries...");

        long lastTime = time.get();
        final MiruTimeRange timeRange = new MiruTimeRange(lastTime - timespan, lastTime);

        for (MiruFieldDefinition fieldDefinition : miruSchema.getFieldDefinitions()) {
            long s = System.currentTimeMillis();
            MiruRequest<UniquesQuery> request = new MiruRequest<>("test",
                    tenant1,
                    MiruActorId.NOT_PROVIDED,
                    MiruAuthzExpression.NOT_PROVIDED,
                    new UniquesQuery(timeRange,
                            fieldDefinition.name,
                            null,
                            MiruFilter.NO_FILTER,
                            null),
                    MiruSolutionLogLevel.INFO);
            MiruResponse<UniquesAnswer> uniquesResult = injectable.gatherUniques(request);

            Assert.assertEquals(uniquesResult.answer.uniques, numberOfUsers);
            System.out.println("uniquesResult: " + uniquesResult);
            System.out.println("took: " + (System.currentTimeMillis() - s));
        }
    }

}
