package com.jivesoftware.os.miru.stream.plugins.fulltext;

import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.bitmaps.roaring6.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.test.MiruPluginTestBootstrap;
import com.jivesoftware.os.miru.service.MiruService;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.BeforeMethod;

/**
 *
 */
public class FullTextGathererTest {

    MiruSchema miruSchema = new MiruSchema.Builder("test", 1)
        .setFieldDefinitions(new MiruFieldDefinition[] {
            new MiruFieldDefinition(0, "user", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
            new MiruFieldDefinition(1, "doc", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
            new MiruFieldDefinition(2, "obj", MiruFieldDefinition.Type.multiTerm,
                new MiruFieldDefinition.Prefix(MiruFieldDefinition.Prefix.Type.numeric, 4, ' ')),
            new MiruFieldDefinition(3, "text", MiruFieldDefinition.Type.multiTermCardinality, MiruFieldDefinition.Prefix.WILDCARD)
        })
        .build();

    MiruTenantId tenant1 = new MiruTenantId("tenant1".getBytes());
    MiruPartitionId partitionId = MiruPartitionId.of(1);
    MiruHost miruHost = new MiruHost("logicalName");
    AtomicInteger walIndex = new AtomicInteger();

    private final int numberOfQueries = 2;
    private final int numberOfTermsPerQuery = 10;
    private final int numberOfUsers = 10;
    private final int numberOfDocsPerUser = 2;
    private final int numberOfTermsPerDoc = 100;
    private final int numberOfActivities = numberOfUsers * numberOfDocsPerUser;
    private final int numberOfBuckets = 32;

    private MiruService service;
    private FullTextInjectable injectable;

    private final MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();
    private final String[] dictionary = new String[10];

    @BeforeMethod
    public void setUpMethod() throws Exception {
        MiruProvider<MiruService> miruProvider = new MiruPluginTestBootstrap().bootstrap(tenant1, partitionId, miruHost,
            miruSchema, MiruBackingStorage.disk, new MiruBitmapsRoaring(), Collections.emptyList());

        this.service = miruProvider.getMiru(tenant1);
        this.injectable = new FullTextInjectable(miruProvider, new FullText(miruProvider));

        for (int i = 0; i < dictionary.length; i++) {
            dictionary[i] = Integer.toHexString(i);
        }


    }


}