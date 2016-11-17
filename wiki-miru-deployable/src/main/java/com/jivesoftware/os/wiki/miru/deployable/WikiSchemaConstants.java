package com.jivesoftware.os.wiki.miru.deployable;

import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Prefix;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Type;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;

/**
 * Support a graph data struture. Anything can be a node with a GUID, type, title, body, properties
 */
public class WikiSchemaConstants {

    private WikiSchemaConstants() {
    }

    public static final MiruSchema SCHEMA = new MiruSchema.Builder("wiki", 4).setFieldDefinitions(new MiruFieldDefinition[] {
        new MiruFieldDefinition(0, "locale", Type.singleTermNonStored, Prefix.NONE),
        new MiruFieldDefinition(1, "auth", Type.singleTerm, Prefix.NONE), // *:*:*:* = <public>:<folderPublic>:<userPublic>:<
        new MiruFieldDefinition(2, "userGuid", Type.singleTerm, Prefix.NONE),
        new MiruFieldDefinition(3, "folderGuid", Type.singleTerm, Prefix.WILDCARD),
        new MiruFieldDefinition(4, "guid", Type.singleTerm, Prefix.WILDCARD),
        new MiruFieldDefinition(5, "verb", Type.singleTerm, Prefix.NONE),
        new MiruFieldDefinition(6, "type", Type.singleTerm, Prefix.WILDCARD),
        new MiruFieldDefinition(7, "title", Type.multiTermNonStored, Prefix.WILDCARD),
        new MiruFieldDefinition(8, "body", Type.multiTermNonStored, Prefix.WILDCARD),
        new MiruFieldDefinition(9, "bodyGuid", Type.singleTerm, Prefix.NONE),
        new MiruFieldDefinition(10, "properties", Type.multiTerm, Prefix.WILDCARD),
        new MiruFieldDefinition(11, "edgeGuids", Type.multiTerm, Prefix.WILDCARD)
    }).build();

}
