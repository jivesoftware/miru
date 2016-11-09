package com.jivesoftware.os.wiki.miru.deployable;

import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Prefix;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Type;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;

/**
 *
 */
public class WikiSchemaConstants {

    private WikiSchemaConstants() {
    }

    public static final MiruSchema SCHEMA = new MiruSchema.Builder("wiki", 3).setFieldDefinitions(new MiruFieldDefinition[]{
        new MiruFieldDefinition(0, "id", MiruFieldDefinition.Type.singleTerm, Prefix.WILDCARD),
        new MiruFieldDefinition(1, "subject", Type.multiTermNonStored, Prefix.WILDCARD),
        new MiruFieldDefinition(2, "body", Type.multiTermNonStored, Prefix.WILDCARD)
    }).build();

}
