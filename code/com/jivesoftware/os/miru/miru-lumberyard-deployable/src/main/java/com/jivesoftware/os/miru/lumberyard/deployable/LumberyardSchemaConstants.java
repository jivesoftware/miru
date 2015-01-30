package com.jivesoftware.os.miru.lumberyard.deployable;

import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Prefix;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;

import static com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Type.multiTerm;
import static com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Type.singleTerm;

/**
 *
 */
public class LumberyardSchemaConstants {

    private LumberyardSchemaConstants() {
    }

    public static final Prefix TYPED_PREFIX = new Prefix(Prefix.Type.numeric, 4, ' ');

    public static final MiruSchema SCHEMA = new MiruSchema.Builder("lumberyard", 1)
        .setFieldDefinitions(new MiruFieldDefinition[]{
            new MiruFieldDefinition(0, "serviceId", singleTerm, Prefix.NONE),
            new MiruFieldDefinition(1, "level", singleTerm, Prefix.NONE),
            new MiruFieldDefinition(2, "thread", singleTerm, Prefix.NONE),
            new MiruFieldDefinition(3, "loggerName", singleTerm, Prefix.NONE),
            new MiruFieldDefinition(4, "message", multiTerm, Prefix.WILDCARD)
        }).build();

}
