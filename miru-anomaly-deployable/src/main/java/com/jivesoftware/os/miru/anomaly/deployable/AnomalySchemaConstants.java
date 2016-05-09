package com.jivesoftware.os.miru.anomaly.deployable;

import com.google.common.base.Charsets;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Prefix;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

import static com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Type.multiTerm;
import static com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Type.singleTerm;

/**
 *
 */
public class AnomalySchemaConstants {

    private AnomalySchemaConstants() {
    }

    public static final MiruTenantId TENANT_ID = new MiruTenantId("anomaly".getBytes(Charsets.UTF_8));

    public static final MiruSchema SCHEMA = new MiruSchema.Builder("anomaly", 2)
        .setFieldDefinitions(new MiruFieldDefinition[]{
            new MiruFieldDefinition(0, "datacenter", singleTerm, Prefix.NONE),
            new MiruFieldDefinition(1, "cluster", singleTerm, Prefix.NONE),
            new MiruFieldDefinition(2, "host", singleTerm, Prefix.WILDCARD),
            new MiruFieldDefinition(3, "service", singleTerm, Prefix.WILDCARD),
            new MiruFieldDefinition(4, "instance", singleTerm, Prefix.NONE),
            new MiruFieldDefinition(5, "version", singleTerm, Prefix.NONE),
            new MiruFieldDefinition(6, "sampler", singleTerm, Prefix.WILDCARD),
            new MiruFieldDefinition(7, "metric", singleTerm, Prefix.WILDCARD),
            new MiruFieldDefinition(8, "bits", multiTerm, Prefix.NONE),
            new MiruFieldDefinition(9, "tags", multiTerm, Prefix.WILDCARD),
            new MiruFieldDefinition(10, "type", singleTerm, Prefix.NONE),
            new MiruFieldDefinition(11, "timestamp", singleTerm, Prefix.NONE),
            new MiruFieldDefinition(12, "tenant", singleTerm, Prefix.WILDCARD)
        }).build();
}
