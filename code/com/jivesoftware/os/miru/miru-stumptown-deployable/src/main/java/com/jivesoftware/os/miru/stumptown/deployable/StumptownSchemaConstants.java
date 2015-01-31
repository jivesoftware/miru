package com.jivesoftware.os.miru.stumptown.deployable;

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
public class StumptownSchemaConstants {


    private StumptownSchemaConstants() {
    }

    public static final MiruTenantId TENANT_ID = new MiruTenantId("stumptown".getBytes(Charsets.UTF_8));

    public static final MiruSchema SCHEMA = new MiruSchema.Builder("stumptown", 2)
        .setFieldDefinitions(new MiruFieldDefinition[] {
            new MiruFieldDefinition(0, "datacenter", singleTerm, Prefix.NONE),
            new MiruFieldDefinition(1, "cluster", singleTerm, Prefix.NONE),
            new MiruFieldDefinition(2, "host", singleTerm, Prefix.WILDCARD),
            new MiruFieldDefinition(3, "service", singleTerm, Prefix.WILDCARD),
            new MiruFieldDefinition(4, "instance", singleTerm, Prefix.NONE),
            new MiruFieldDefinition(5, "version", singleTerm, Prefix.NONE),
            new MiruFieldDefinition(6, "level", singleTerm, Prefix.NONE),
            new MiruFieldDefinition(7, "thread", singleTerm, Prefix.WILDCARD),
            new MiruFieldDefinition(8, "logger", singleTerm, Prefix.WILDCARD),
            new MiruFieldDefinition(9, "message", multiTerm, Prefix.WILDCARD),
            new MiruFieldDefinition(10, "timestamp", singleTerm, Prefix.NONE),
            new MiruFieldDefinition(11, "thrownStackTrace", multiTerm, Prefix.WILDCARD)
        }).build();

}
