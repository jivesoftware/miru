package com.jivesoftware.os.wiki.miru.deployable;

import com.google.common.collect.ImmutableMap;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Prefix;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Type;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import java.util.HashMap;
import java.util.Map;

/**
 * Support a graph data struture. Anything can be a node with a GUID, type, title, body, properties
 */
public class WikiSchemaConstants {

    private WikiSchemaConstants() {
    }

    public static final MiruSchema SCHEMA = new MiruSchema.Builder("wiki", 1).setFieldDefinitions(new MiruFieldDefinition[] {
        new MiruFieldDefinition(0, "locale", Type.singleTermNonStored, Prefix.NONE),
        new MiruFieldDefinition(1, "auth", Type.singleTerm, Prefix.NONE), // *:*:*:* = <public>:<folderPublic>:<userPublic>:<
        new MiruFieldDefinition(2, "userGuid", Type.singleTerm, Prefix.NONE),
        new MiruFieldDefinition(3, "folderGuid", Type.singleTerm, Prefix.NONE),
        new MiruFieldDefinition(4, "guid", Type.singleTerm, Prefix.NONE),
        new MiruFieldDefinition(5, "verb", Type.singleTerm, Prefix.NONE),
        new MiruFieldDefinition(6, "type", Type.singleTerm, Prefix.WILDCARD),
        new MiruFieldDefinition(7, "title", Type.multiTermNonStored, Prefix.WILDCARD),
        new MiruFieldDefinition(8, "body", Type.multiTermNonStored, Prefix.WILDCARD)
    }).build();


    public static final Map<String, Object> esSchema() {
        Map<String, Object> source = new HashMap<>();
        Map<String, Object> properties = new HashMap<>();
        source.put("properties", properties);
        source.put("_all", ImmutableMap.of("enabled", "false"));

        properties.put("tenant", ImmutableMap.of("type", "string"));
        properties.put("locale", ImmutableMap.of("type", "string"));
        properties.put("auth", ImmutableMap.of("type", "string", "store", "yes"));
        properties.put("userGuid", ImmutableMap.of("type", "string", "store", "yes"));
        properties.put("folderGuid", ImmutableMap.of("type", "string", "store", "yes"));
        properties.put("guid", ImmutableMap.of("type", "string", "store", "yes"));
        properties.put("verb", ImmutableMap.of("type", "string", "store", "yes"));
        properties.put("type", ImmutableMap.of("type", "string", "store", "yes"));
        properties.put("title", ImmutableMap.of("type", "text", "analyzer", "english"));
        properties.put("body", ImmutableMap.of("type", "text", "analyzer", "english"));

        return source;
    }

}
