package com.jivesoftware.os.miru.stream.plugins.benchmark;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.query.MiruTimeRange;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsQuery;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.jivesoftware.os.miru.stream.plugins.benchmark.MiruStreamServiceBenchmarkUtils.generateActivity;

public enum MiruCustomerSize {
    SMALL_CUSTOMER {
        private final int numActivities = 10_000;

        private final Map<MiruFieldName, Integer> fieldNameToTotalCount = ImmutableMap.<MiruFieldName, Integer>builder()
            .put(MiruFieldName.ACTIVITY_PARENT, 5_000)
            .put(MiruFieldName.AUTHOR_ID, 100)
            .put(MiruFieldName.CONTAINER_ID, 25)
            .put(MiruFieldName.MENTIONED_CONTAINER_IDS, 5)
            .put(MiruFieldName.MENTIONED_USER_IDS, 35)
            .put(MiruFieldName.META_CLASS_NAME, 3)
            .put(MiruFieldName.META_ID, 3)
            .put(MiruFieldName.OBJECT_ID, 10_000)
            .put(MiruFieldName.PARTICIPANT_IDS, 100)
            .put(MiruFieldName.STATUS, 0) // TODO - Figure out if we need this/what this is
            .put(MiruFieldName.TAG_IDS, 500)
            .put(MiruFieldName.VERB_SUBJECT_CLASS_NAME, 10)
            .put(MiruFieldName.VIEW_CLASS_NAME, 5)
            .build();

        @Override
        int getCapacity() {
            return numActivities;
        }

        @Override
        Map<MiruFieldName, Integer> getFieldNameToTotalCount() {
            return fieldNameToTotalCount;
        }

        @Override
        long generateAndIndexActivities(MiruService miruService, OrderIdProvider orderIdProvider, Random random, MiruTenantId tenantId,
            MiruFieldCardinality fieldCardinality) throws Exception {
            List<MiruPartitionedActivity> miruActivities = Lists.newArrayListWithExpectedSize(500);

            String[] authz = new String[0]; // TODO - include authz?
            for (int i = 0; i < numActivities; i++) {
                if (miruActivities.size() == 500) {
                    miruService.writeToIndex(ImmutableList.copyOf(miruActivities));
                    miruActivities.clear();
                }
                miruActivities.add(generateActivity(orderIdProvider, random, tenantId, authz, fieldCardinality, fieldNameToTotalCount));
            }
            miruService.writeToIndex(ImmutableList.copyOf(miruActivities));
            miruActivities.clear();

            return orderIdProvider.nextId();
        }

        @Override
        List<AggregateCountsQuery> generateInboxAggregateCountsQueries(Random random, MiruTenantId tenantId, int distinctQueries,
            MiruFieldCardinality fieldCardinality, MiruFollowables followables, long maxOrderId) {

            return doGenerateInboxAggregateCountsQueries(random, tenantId, distinctQueries, this, followables, maxOrderId);
        }
    },

    MEDIUM_CUSTOMER {
        private final int numActivities = 100_000;

        public final Map<MiruFieldName, Integer> fieldNameToTotalCount = ImmutableMap.<MiruFieldName, Integer>builder()
            .put(MiruFieldName.ACTIVITY_PARENT, 50_000)
            .put(MiruFieldName.AUTHOR_ID, 1_000)
            .put(MiruFieldName.CONTAINER_ID, 500)
            .put(MiruFieldName.MENTIONED_CONTAINER_IDS, 100)
            .put(MiruFieldName.MENTIONED_USER_IDS, 350)
            .put(MiruFieldName.META_CLASS_NAME, 3)
            .put(MiruFieldName.META_ID, 3)
            .put(MiruFieldName.OBJECT_ID, 100_000)
            .put(MiruFieldName.PARTICIPANT_IDS, 1_000)
            .put(MiruFieldName.STATUS, 0) // TODO - Figure out if we need this/what this is
            .put(MiruFieldName.TAG_IDS, 750)
            .put(MiruFieldName.VERB_SUBJECT_CLASS_NAME, 10)
            .put(MiruFieldName.VIEW_CLASS_NAME, 5)
            .build();

        @Override
        int getCapacity() {
            return numActivities;
        }

        @Override
        Map<MiruFieldName, Integer> getFieldNameToTotalCount() {
            return fieldNameToTotalCount;
        }

        @Override
        long generateAndIndexActivities(MiruService miruService, OrderIdProvider orderIdProvider, Random random, MiruTenantId tenantId,
            MiruFieldCardinality fieldCardinality) throws Exception {
            List<MiruPartitionedActivity> miruActivities = Lists.newArrayListWithExpectedSize(500);

            String[] authz = new String[0]; // TODO - include authz?
            for (int i = 0; i < numActivities; i++) {
                if (miruActivities.size() == 500) {
                    miruService.writeToIndex(ImmutableList.copyOf(miruActivities));
                    miruActivities.clear();
                }
                miruActivities.add(generateActivity(orderIdProvider, random, tenantId, authz, fieldCardinality, fieldNameToTotalCount));
            }
            miruService.writeToIndex(ImmutableList.copyOf(miruActivities));
            miruActivities.clear();

            return orderIdProvider.nextId();
        }

        @Override
        List<AggregateCountsQuery> generateInboxAggregateCountsQueries(Random random, MiruTenantId tenantId, int distinctQueries,
            MiruFieldCardinality fieldCardinality, MiruFollowables followables, long maxOrderId) {

            return doGenerateInboxAggregateCountsQueries(random, tenantId, distinctQueries, this, followables, maxOrderId);
        }
    },

    LARGE_CUSTOMER {
        private final int numActivities = 1_000_000;

        private final Map<MiruFieldName, Integer> fieldNameToTotalCount = ImmutableMap.<MiruFieldName, Integer>builder()
            .put(MiruFieldName.ACTIVITY_PARENT, 500_000)
            .put(MiruFieldName.AUTHOR_ID, 10_000)
            .put(MiruFieldName.CONTAINER_ID, 5_000)
            .put(MiruFieldName.MENTIONED_CONTAINER_IDS, 1_000)
            .put(MiruFieldName.MENTIONED_USER_IDS, 2_500)
            .put(MiruFieldName.META_CLASS_NAME, 3)
            .put(MiruFieldName.META_ID, 3)
            .put(MiruFieldName.OBJECT_ID, 1_000_000)
            .put(MiruFieldName.PARTICIPANT_IDS, 10_000)
            .put(MiruFieldName.STATUS, 0) // TODO - Figure out if we need this/what this is
            .put(MiruFieldName.TAG_IDS, 1_500)
            .put(MiruFieldName.VERB_SUBJECT_CLASS_NAME, 10)
            .put(MiruFieldName.VIEW_CLASS_NAME, 5)
            .build();

        @Override
        int getCapacity() {
            return numActivities;
        }

        @Override
        Map<MiruFieldName, Integer> getFieldNameToTotalCount() {
            return fieldNameToTotalCount;
        }

        @Override
        long generateAndIndexActivities(MiruService miruService, OrderIdProvider orderIdProvider, Random random, MiruTenantId tenantId,
            MiruFieldCardinality fieldCardinality) throws Exception {
            List<MiruPartitionedActivity> miruActivities = Lists.newArrayListWithExpectedSize(500);

            String[] authz = new String[0]; // TODO - include authz?
            for (int i = 0; i < numActivities; i++) {
                if (miruActivities.size() == 500) {
                    miruService.writeToIndex(ImmutableList.copyOf(miruActivities));
                    miruActivities.clear();
                }
                miruActivities.add(generateActivity(orderIdProvider, random, tenantId, authz, fieldCardinality, fieldNameToTotalCount));
            }
            miruService.writeToIndex(ImmutableList.copyOf(miruActivities));
            miruActivities.clear();

            return orderIdProvider.nextId();
        }

        @Override
        List<AggregateCountsQuery> generateInboxAggregateCountsQueries(Random random, MiruTenantId tenantId, int distinctQueries,
            MiruFieldCardinality fieldCardinality, MiruFollowables followables, long maxOrderId) {

            return doGenerateInboxAggregateCountsQueries(random, tenantId, distinctQueries, this, followables, maxOrderId);
        }
    };

    private static List<AggregateCountsQuery> doGenerateInboxAggregateCountsQueries(Random random, MiruTenantId miruTenantId, int distinctQueries,
        MiruCustomerSize miruCustomerSize, MiruFollowables followables, long maxOrderId) {
        List<AggregateCountsQuery> aggregateCountsQueries = Lists.newArrayListWithExpectedSize(distinctQueries);

        MiruStreamId streamId = new MiruStreamId(FilerIO.longBytes(1));
        for (int i = 0; i < distinctQueries; i++) {
            MiruFilter filter = new MiruFilter(MiruFilterOperation.or,
                Optional.of(ImmutableList.copyOf(followables.getFieldFilters(random, miruCustomerSize))),
                Optional.<ImmutableList<MiruFilter>>absent());

            AggregateCountsQuery aggregateCountsQuery = new AggregateCountsQuery(miruTenantId,
                Optional.of(streamId),
                Optional.of(new MiruTimeRange(0, maxOrderId)),
                Optional.of(new MiruTimeRange(0, maxOrderId)),
                Optional.<MiruAuthzExpression>absent(), // TODO - include authz?
                filter,
                Optional.<MiruFilter>absent(),
                "",
                MiruFieldName.ACTIVITY_PARENT.getFieldName(),
                0, 51);
            aggregateCountsQueries.add(aggregateCountsQuery);
        }

        return aggregateCountsQueries;
    }

    abstract int getCapacity();

    abstract Map<MiruFieldName, Integer> getFieldNameToTotalCount();

    abstract long generateAndIndexActivities(MiruService miruService, OrderIdProvider orderIdProvider, Random random, MiruTenantId tenantId,
        MiruFieldCardinality fieldCardinality) throws Exception;

    abstract List<AggregateCountsQuery> generateInboxAggregateCountsQueries(Random random, MiruTenantId tenantId, int distinctQueries,
        MiruFieldCardinality fieldCardinality, MiruFollowables followables, long maxOrderId);
}