package com.jivesoftware.os.miru.reco.plugins;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import com.jivesoftware.os.miru.bitmaps.roaring5.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.test.MiruPluginTestBootstrap;
import com.jivesoftware.os.miru.reco.plugins.distincts.Distincts;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsAnswer;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsInjectable;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;
import com.jivesoftware.os.miru.service.MiruService;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author jonathan
 */
public class MiruEjjiNGTest {

    MiruSchema miruSchema = new MiruSchema.Builder("test", 1)
        .setFieldDefinitions(new MiruFieldDefinition[] {
            new MiruFieldDefinition(0, "user", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
            new MiruFieldDefinition(1, "doc", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
            new MiruFieldDefinition(2, "docType", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
            new MiruFieldDefinition(3, "stream", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
            new MiruFieldDefinition(4, "source", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE)
        })
        .build();

    MiruTenantId tenant1 = new MiruTenantId("tenant1".getBytes());
    MiruPartitionId partitionId = MiruPartitionId.of(1);
    MiruHost miruHost = new MiruHost("logicalName");
    CollaborativeFilterUtil util = new CollaborativeFilterUtil();
    AtomicInteger walIndex = new AtomicInteger();

    int numqueries = 10_000;
    int numberOfUsers = 1_000;
    int numberOfDocument = 1_000_000;
    int numberOfDocumentType = 4;
    int numberOfFollows = 2_600_000;
    int numberOfStreamsPerUser = 5;
    int numberOfStreamSources = 4;
    int numberOfActivities = numberOfFollows;
    int numberOfBuckets = 32;

    MiruService service;
    DistinctsInjectable injectable;

    @BeforeMethod
    public void setUpMethod() throws Exception {
        MiruProvider<MiruService> miruProvider = new MiruPluginTestBootstrap().bootstrap(tenant1, partitionId, miruHost,
            miruSchema, MiruBackingStorage.disk, new MiruBitmapsRoaring(), Collections.emptyList());

        this.service = miruProvider.getMiru(tenant1);

        this.injectable = new DistinctsInjectable(miruProvider, new Distincts(miruProvider.getTermComposer()));
    }

    @Test(enabled = false)
    public void basicTest() throws Exception {

        Random rand = new Random(1_234);
        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        long timespan = numberOfBuckets * snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(3), 0, 0);
        long intervalPerActivity = timespan / numberOfActivities;
        long initialTime = snowflakeIdPacker.pack(System.currentTimeMillis(), 0, 0) - timespan;
        AtomicLong time = new AtomicLong(initialTime);
        System.out.println("Building activities....");

        Set<String> users = Sets.newHashSet();
        Set<String> streams = Sets.newHashSet();
        Set<Integer> types = Sets.newHashSet();

        List<MiruPartitionedActivity> activities = Lists.newArrayList();
        for (int i = 0; i < numberOfActivities; i++) {
            String user = "bob" + rand.nextInt(numberOfUsers);
            users.add(user);

            int streamId = rand.nextInt(numberOfStreamsPerUser);
            String stream = user + "_" + streamId;
            streams.add(stream);
            String source = String.valueOf(streamId % numberOfStreamSources);

            int docId = rand.nextInt(numberOfDocument);
            int docType = docId % numberOfDocumentType;
            types.add(docType);
            String doc = "doc_" + docType + "_" + docId;

            activities.add(buildActivity(tenant1,
                partitionId,
                time.addAndGet(intervalPerActivity),
                user,
                doc,
                String.valueOf(docType),
                stream,
                source,
                walIndex.incrementAndGet()));

            if (activities.size() == 10_000) {
                service.writeToIndex(activities);
                activities.clear();
                System.out.println("Finished " + i + " activities");
            }
        }

        if (!activities.isEmpty()) {
            service.writeToIndex(activities);
        }

        System.out.println("Running queries...");

        List<String> orderedUsers = Lists.newArrayList(users);
        List<Integer> orderedTypes = Lists.newArrayList(types);

        for (int i = 0; i < numqueries; i++) {
            long s = System.currentTimeMillis();
            String user = orderedUsers.get(rand.nextInt(orderedUsers.size()));
            List<String> docTypes = Arrays.asList(
                String.valueOf(orderedTypes.get(rand.nextInt(orderedTypes.size()))),
                String.valueOf(orderedTypes.get(rand.nextInt(orderedTypes.size()))),
                String.valueOf(orderedTypes.get(rand.nextInt(orderedTypes.size()))));
            List<String> sources = Arrays.asList(
                String.valueOf(rand.nextInt(numberOfStreamSources)),
                String.valueOf(rand.nextInt(numberOfStreamSources)));
            MiruRequest<DistinctsQuery> request = new MiruRequest<>("test",
                tenant1,
                MiruActorId.NOT_PROVIDED,
                MiruAuthzExpression.NOT_PROVIDED,
                new DistinctsQuery(new MiruTimeRange(initialTime - 1, time.get() + 1),
                    "doc",
                    null,
                    new MiruFilter(MiruFilterOperation.and,
                        false,
                        Arrays.asList(
                            MiruFieldFilter.ofTerms(MiruFieldType.primary, "user", user),
                            MiruFieldFilter.of(MiruFieldType.primary, "docType", docTypes),
                            MiruFieldFilter.of(MiruFieldType.primary, "source", sources)),
                        null),
                    null),
                MiruSolutionLogLevel.NONE);
            MiruResponse<DistinctsAnswer> distinctsResult = injectable.gatherDistincts(request);

            System.out.println("distinctsResult:" + distinctsResult.answer.results.size());
            System.out.println("Took:" + (System.currentTimeMillis() - s));
        }

    }

    private final MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();

    public MiruPartitionedActivity buildActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        long time,
        String user,
        String doc,
        String docType,
        String stream,
        String source,
        int index) {

        Map<String, List<String>> fieldsValues = Maps.newHashMap();
        fieldsValues.put("user", Arrays.asList(user));
        fieldsValues.put("doc", Arrays.asList(doc));
        fieldsValues.put("docType", Arrays.asList(docType));
        fieldsValues.put("stream", Arrays.asList(stream));
        fieldsValues.put("source", Arrays.asList(source));

        MiruActivity activity = new MiruActivity(tenantId, time, 0, false, new String[0], fieldsValues, Collections.emptyMap());
        return partitionedActivityFactory.activity(1, partitionId, index, activity);
    }
}
