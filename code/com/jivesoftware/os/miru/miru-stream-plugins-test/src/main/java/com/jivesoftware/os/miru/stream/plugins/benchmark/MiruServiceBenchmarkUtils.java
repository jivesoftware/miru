package com.jivesoftware.os.miru.stream.plugins.benchmark;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.activity.schema.DefaultMiruSchemaDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;


public class MiruServiceBenchmarkUtils {

    private static final MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();

    public static MiruPartitionedActivity generateActivity(OrderIdProvider orderIdProvider, Random random, MiruTenantId tenantId, String[] authz,
            MiruFieldCardinality fieldCardinality, Map<MiruFieldName, Integer> fieldNameToTotalCount) {

        Map<String, List<String>> fieldsValues = Maps.newHashMap();
        for (MiruFieldDefinition fieldDefinition : DefaultMiruSchemaDefinition.FIELDS) {
            int termFrequency = 1; // TODO - Figure out if we need come up with real term frequencies

            MiruFieldName miruFieldName = MiruFieldName.fieldNameToMiruFieldName(fieldDefinition.name);
            Integer totalCount = fieldNameToTotalCount.get(miruFieldName);
            if (totalCount == null) {
                continue;
            }

            int cardinality = fieldCardinality.getCardinality(miruFieldName, totalCount);
            List<String> terms = generateDisticts(random, termFrequency, cardinality);
            fieldsValues.put(fieldDefinition.name, terms);
        }

        long time = orderIdProvider.nextId();
        MiruActivity activity = new MiruActivity(tenantId, time, authz, 0,
                fieldsValues, Collections.<String, List<String>>emptyMap());
        return partitionedActivityFactory.activity(1, MiruPartitionId.of(1), 1, activity); // HACK
    }

    public static List<String> generateDisticts(Random random, int termFrequency, int cardinality) {
        Set<String> usedTerms = Sets.newHashSet();
        List<String> distincts = new ArrayList<>();

        // Short-circuit when we have high cardinality
        if (termFrequency == cardinality) {
            for (int i = 0; i < cardinality; i++) {
                distincts.add(String.valueOf(i));
            }
            return distincts;
        }

        while (distincts.size() < termFrequency) {
            int term = random.nextInt(cardinality);
            if (usedTerms.add(String.valueOf(term))) {
                distincts.add(String.valueOf(term));
            }
        }
        return distincts;
    }
}
