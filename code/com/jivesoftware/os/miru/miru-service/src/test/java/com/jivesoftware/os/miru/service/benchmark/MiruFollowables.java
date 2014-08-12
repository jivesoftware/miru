package com.jivesoftware.os.miru.service.benchmark;

import com.google.common.collect.ImmutableList;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import java.util.Map;
import java.util.Random;

import static com.jivesoftware.os.miru.service.benchmark.MiruStreamServiceBenchmarkUtils.generateDisticts;

public enum MiruFollowables {

    SMALL {
        @Override
        ImmutableList<MiruFieldFilter> getFieldFilters(Random random, MiruCustomerSize customerSize) {
            Map<MiruFieldName, Integer> fieldNameToTotalCount = customerSize.getFieldNameToTotalCount();

            ImmutableList<MiruTermId> authorValues = getFilterValues(fieldNameToTotalCount, random, 0.05, MiruFieldName.AUTHOR_ID);
            ImmutableList<MiruTermId> containerValues = getFilterValues(fieldNameToTotalCount, random, 0.01, MiruFieldName.CONTAINER_ID);
            ImmutableList<MiruTermId> activityParentValues = getFilterValues(fieldNameToTotalCount, random, 0.10, MiruFieldName.ACTIVITY_PARENT);
            ImmutableList<MiruTermId> tagValues = getFilterValues(fieldNameToTotalCount, random, 0.01, MiruFieldName.TAG_IDS);

            return ImmutableList.of(
                new MiruFieldFilter(MiruFieldName.AUTHOR_ID.getFieldName(), authorValues),
                new MiruFieldFilter(MiruFieldName.CONTAINER_ID.getFieldName(), containerValues),
                new MiruFieldFilter(MiruFieldName.ACTIVITY_PARENT.getFieldName(), activityParentValues),
                new MiruFieldFilter(MiruFieldName.TAG_IDS.getFieldName(), tagValues)
            );
        }
    },
    MEDIUM {
        @Override
        ImmutableList<MiruFieldFilter> getFieldFilters(Random random, MiruCustomerSize customerSize) {
            Map<MiruFieldName, Integer> fieldNameToTotalCount = customerSize.getFieldNameToTotalCount();

            ImmutableList<MiruTermId> authorValues = getFilterValues(fieldNameToTotalCount, random, 0.1, MiruFieldName.AUTHOR_ID);
            ImmutableList<MiruTermId> containerValues = getFilterValues(fieldNameToTotalCount, random, 0.5, MiruFieldName.CONTAINER_ID);
            ImmutableList<MiruTermId> activityParentValues = getFilterValues(fieldNameToTotalCount, random, 0.20, MiruFieldName.ACTIVITY_PARENT);
            ImmutableList<MiruTermId> tagValues = getFilterValues(fieldNameToTotalCount, random, 0.05, MiruFieldName.TAG_IDS);

            return ImmutableList.of(
                new MiruFieldFilter(MiruFieldName.AUTHOR_ID.getFieldName(), authorValues),
                new MiruFieldFilter(MiruFieldName.CONTAINER_ID.getFieldName(), containerValues),
                new MiruFieldFilter(MiruFieldName.ACTIVITY_PARENT.getFieldName(), activityParentValues),
                new MiruFieldFilter(MiruFieldName.TAG_IDS.getFieldName(), tagValues)
            );
        }
    },
    LARGE {
        @Override
        ImmutableList<MiruFieldFilter> getFieldFilters(Random random, MiruCustomerSize customerSize) {
            Map<MiruFieldName, Integer> fieldNameToTotalCount = customerSize.getFieldNameToTotalCount();

            ImmutableList<MiruTermId> authorValues = getFilterValues(fieldNameToTotalCount, random, 0.25, MiruFieldName.AUTHOR_ID);
            ImmutableList<MiruTermId> containerValues = getFilterValues(fieldNameToTotalCount, random, 0.10, MiruFieldName.CONTAINER_ID);
            ImmutableList<MiruTermId> activityParentValues = getFilterValues(fieldNameToTotalCount, random, 0.30, MiruFieldName.ACTIVITY_PARENT);
            ImmutableList<MiruTermId> tagValues = getFilterValues(fieldNameToTotalCount, random, 0.1, MiruFieldName.TAG_IDS);

            return ImmutableList.of(
                new MiruFieldFilter(MiruFieldName.AUTHOR_ID.getFieldName(), authorValues),
                new MiruFieldFilter(MiruFieldName.CONTAINER_ID.getFieldName(), containerValues),
                new MiruFieldFilter(MiruFieldName.ACTIVITY_PARENT.getFieldName(), activityParentValues),
                new MiruFieldFilter(MiruFieldName.TAG_IDS.getFieldName(), tagValues)
            );
        }
    };

    private static ImmutableList<MiruTermId> getFilterValues(Map<MiruFieldName, Integer> fieldNameToTotalCount,
        Random random, double termFrequencyPercentage, MiruFieldName fieldName) {

        int cardinality = fieldNameToTotalCount.get(fieldName);
        int termFrequency = (int) (cardinality * termFrequencyPercentage);
        if (termFrequency == 0) {
            termFrequency = 1;
        }

        return ImmutableList.copyOf(generateDisticts(random, termFrequency, cardinality));
    }

    abstract ImmutableList<MiruFieldFilter> getFieldFilters(Random random, MiruCustomerSize customerSize);
}
