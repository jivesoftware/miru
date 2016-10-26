package com.jivesoftware.os.wiki.miru.deployable;

import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Prefix;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;

/**
 *
 */
public class WikiSchemaConstants {

    private WikiSchemaConstants() {
    }

    public static final MiruSchema SCHEMA = new MiruSchema.Builder("wiki", 2).setFieldDefinitions(new MiruFieldDefinition[]{
        new MiruFieldDefinition(1, "id", MiruFieldDefinition.Type.singleTerm, Prefix.WILDCARD),
        new MiruFieldDefinition(2, "subject", MiruFieldDefinition.Type.multiTerm, Prefix.WILDCARD),
        new MiruFieldDefinition(3, "body", MiruFieldDefinition.Type.multiTerm, Prefix.WILDCARD)
    }).build();

}
