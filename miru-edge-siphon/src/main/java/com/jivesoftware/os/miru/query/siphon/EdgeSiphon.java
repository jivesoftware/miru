package com.jivesoftware.os.miru.query.siphon;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Prefix;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Prefix.Type;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Type.multiTerm;
import static com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Type.nonIndexed;
import static com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Type.singleTerm;

/**
 * Created by jonathan.colt on 5/1/17.
 */
public class EdgeSiphon implements MiruSiphonPlugin {

    public static ObjectMapper mapper = new ObjectMapper();


    private static Prefix LONG = new Prefix(Type.numeric, 8, 0);

    public static final MiruSchema SCHEMA = new MiruSchema.Builder("edgeSiphon", 1)
        .setFieldDefinitions(new MiruFieldDefinition[] {
            new MiruFieldDefinition(0, "id", nonIndexed, LONG),
            new MiruFieldDefinition(1, "tenant", singleTerm, Prefix.NONE),
            new MiruFieldDefinition(2, "user", singleTerm, Prefix.NONE),
            new MiruFieldDefinition(3, "name", singleTerm, Prefix.NONE),
            new MiruFieldDefinition(4, "origin", singleTerm, Prefix.NONE),
            new MiruFieldDefinition(5, "destination", singleTerm, Prefix.NONE),
            new MiruFieldDefinition(6, "tags", multiTerm, Prefix.WILDCARD),
            new MiruFieldDefinition(7, "latency", nonIndexed, LONG)
        }).build();


    @Override
    public String name() {
        return "edgeSiphon";
    }

    @Override
    public MiruSchema schema(MiruTenantId tenantId) throws Exception {
        return SCHEMA;
    }

    @Override
    public ListMultimap<MiruTenantId, MiruActivity> siphon(MiruTenantId tenantId,
        long rowTxId,
        byte[] prefix,
        byte[] key,
        byte[] value,
        long valueTimestamp,
        boolean valueTombstoned,
        long valueVersion) throws Exception {

        Edge edge = mapper.readValue(value, Edge.class);

        ListMultimap<MiruTenantId, MiruActivity> activityListMultimap = ArrayListMultimap.create();

        Map<String, List<String>> fieldsValues = Maps.newHashMap();
        fieldsValues.put("id", Arrays.asList(String.valueOf(UIO.bytesLong(key))));
        if (edge.tenant != null) {
            fieldsValues.put("tenant", Arrays.asList(edge.tenant));
        }
        if (edge.name != null) {
            fieldsValues.put("name", Arrays.asList(edge.name));
        }
        if (edge.user != null) {
            fieldsValues.put("user", Arrays.asList(edge.user));
        }
        if (edge.origin != null) {
            fieldsValues.put("origin", Arrays.asList(edge.origin));
        }
        if (edge.destination != null) {
            fieldsValues.put("destination", Arrays.asList(edge.destination));
        }
        if (edge.tags != null && !edge.tags.isEmpty()) {
            fieldsValues.put("tags", Lists.newArrayList(edge.tags));
        }
        fieldsValues.put("latency", Arrays.asList(String.valueOf(edge.latency)));
        MiruActivity activity = new MiruActivity(tenantId, edge.timestamp, 0, false, new String[0], fieldsValues, Collections.emptyMap());

        activityListMultimap.put(tenantId, activity);

        return activityListMultimap;
    }
}
