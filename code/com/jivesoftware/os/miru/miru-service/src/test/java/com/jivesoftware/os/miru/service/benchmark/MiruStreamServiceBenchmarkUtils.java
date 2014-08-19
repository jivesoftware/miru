package com.jivesoftware.os.miru.service.benchmark;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
import com.jivesoftware.os.miru.service.index.MiruFieldDefinition;

import java.util.*;

import static com.jivesoftware.os.miru.service.schema.DefaultMiruSchemaDefinition.SCHEMA;

public class MiruStreamServiceBenchmarkUtils {

    private static final MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();

    public static MiruPartitionedActivity generateActivity(OrderIdProvider orderIdProvider, Random random, TenantId tenantId, String[] authz,
            MiruFieldCardinality fieldCardinality, Map<MiruFieldName, Integer> fieldNameToTotalCount) {

        Map<String, MiruTermId[]> fieldsValues = Maps.newHashMap();
        for (MiruFieldDefinition fieldDefinition : SCHEMA) {
            int termFrequency = 1; // TODO - Figure out if we need come up with real term frequencies

            MiruFieldName miruFieldName = MiruFieldName.fieldNameToMiruFieldName(fieldDefinition.name);
            Integer totalCount = fieldNameToTotalCount.get(miruFieldName);
            if (totalCount == null) {
                continue;
            }

            int cardinality = fieldCardinality.getCardinality(miruFieldName, totalCount);
            List<MiruTermId> terms = generateDisticts(random, termFrequency, cardinality);
            fieldsValues.put(fieldDefinition.name, terms.toArray(new MiruTermId[0]));
        }

        long time = orderIdProvider.nextId();
        MiruActivity activity = new MiruActivity.Builder(new MiruTenantId(tenantId.toStringForm().getBytes(Charsets.UTF_8)), time, authz, 0)
                .putFieldsValues(fieldsValues)
                .build();
        return partitionedActivityFactory.activity(1, MiruPartitionId.of(1), 1, activity); // HACK
    }

    public static List<MiruTermId> generateDisticts(Random random, int termFrequency, int cardinality) {
        Set<MiruTermId> usedTerms = Sets.newHashSet();
        List<MiruTermId> distincts = new ArrayList<>();

        // Short-circuit when we have high cardinality
        if (termFrequency == cardinality) {
            for (int i = 0; i < cardinality; i++) {
                byte[] termBytes = FilerIO.intBytes(i);
                distincts.add(new MiruTermId(termBytes));
            }
            return distincts;
        }

        while (distincts.size() < termFrequency) {
            int term = random.nextInt(cardinality);
            byte[] termBytes = FilerIO.intBytes(term);
            if (usedTerms.add(new MiruTermId(termBytes))) {
                distincts.add(new MiruTermId(termBytes));
            }
        }
        return distincts;
    }
}
