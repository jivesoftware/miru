package com.jivesoftware.os.miru.test;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.query.solution.MiruTimeRange;
import com.jivesoftware.os.miru.stream.plugins.count.DistinctCountQuery;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsQuery;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import javax.annotation.Nullable;

/**
 *
 */
public class MiruTestStreamQueryDistributor {

    private final Random random;
    private final MiruTestFeatureSupplier featureSupplier;
    private final int numQueries;
    private final int queryUsers;
    private final int queryContainers;
    private final int numResultsAggregateCounts;
    private final int numResultsDistinctCount;

    private final OneTailedRandomNumber pageNumber = new OneTailedRandomNumber(2.0, 10, 0, 10); // 50% back one page, 0.1% back 10 pages

    public MiruTestStreamQueryDistributor(Random random, MiruTestFeatureSupplier featureSupplier, int numQueries, int queryUsers, int queryContainers,
            int numResultsAggregateCounts, int numResultsDistinctCount) {
        this.random = random;
        this.featureSupplier = featureSupplier;
        this.numQueries = numQueries;
        this.queryUsers = queryUsers;
        this.queryContainers = queryContainers;
        this.numResultsAggregateCounts = numResultsAggregateCounts;
        this.numResultsDistinctCount = numResultsDistinctCount;
    }

    public int getNumQueries() {
        return numQueries;
    }

    public AggregateCountsQuery aggregateCountsQuery(boolean inbox) {
        Id userId = featureSupplier.oldUsers(1).get(0);
        ObjectId user = new ObjectId("User", userId);

        int startFromDistinctN = 0;
        MiruTimeRange answerTimeRange = null;
        MiruTimeRange countTimeRange = null;

        if (random.nextInt(100) < 10) {
            // 10% page, which uses an origin timestamp plus an offset
            answerTimeRange = buildTimeRange(true);
            startFromDistinctN = numResultsAggregateCounts * (int) (1 + pageNumber.get(random));
        }

        if (!inbox) {
            // activity stream applies count time range for zippers
            countTimeRange = buildTimeRange(false);
        }

        Optional<MiruFilter> constraintsFilter = buildConstraintsFilter(inbox, userId);

        return new AggregateCountsQuery(
                featureSupplier.miruTenantId(),
                streamId(inbox, user),
                answerTimeRange,
                countTimeRange,
                new MiruFilter(
                        MiruFilterOperation.and,
                        Optional.of(Arrays.asList(viewClassesFilter())),
                        Optional.of(Arrays.asList(
                                new MiruFilter(
                                        MiruFilterOperation.or,
                                        Optional.of(buildFieldFilters(inbox, userId)),
                                        Optional.<List<MiruFilter>>absent())))),
                constraintsFilter.orNull(),
                new MiruAuthzExpression(Lists.newArrayList(featureSupplier.userAuthz(userId))),
                MiruFieldName.ACTIVITY_PARENT.getFieldName(),
                startFromDistinctN,
                numResultsAggregateCounts + 1); // we usually add 1 for "hasMore"
    }

    public DistinctCountQuery distinctCountQuery(boolean inbox) {
        Id userId = featureSupplier.oldUsers(1).get(0);
        ObjectId user = new ObjectId("User", userId);

        MiruTimeRange timeRange = null;
        if (!inbox) {
            // activity stream gets distinct count after last time viewed
            timeRange = buildTimeRange(false);
        }

        Optional<MiruFilter> constraintsFilter = buildConstraintsFilter(inbox, userId);

        return new DistinctCountQuery(
                featureSupplier.miruTenantId(),
                streamId(inbox, user),
                timeRange,
                new MiruFilter(
                        MiruFilterOperation.and,
                        Optional.of(Arrays.asList(viewClassesFilter())),
                        Optional.of(Arrays.asList(
                                new MiruFilter(
                                        MiruFilterOperation.or,
                                        Optional.of(buildFieldFilters(inbox, userId)),
                                        Optional.<List<MiruFilter>>absent())))),
                constraintsFilter.or(MiruFilter.NO_FILTER),
                new MiruAuthzExpression(Lists.newArrayList(featureSupplier.userAuthz(userId))),
                MiruFieldName.ACTIVITY_PARENT.getFieldName(),
                numResultsDistinctCount + 1); // we usually add 1 for "hasMore"
    }

    private MiruStreamId streamId(boolean inbox, ObjectId user) {
        if (inbox) {
            return new MiruStreamId(CompositeId.createOrdered("InboxStream", featureSupplier.tenantId(), user.toStringForm()).toBytes());
        } else {
            return new MiruStreamId(CompositeId.createOrdered("ConnectionsStream", featureSupplier.tenantId(), user.toStringForm()).toBytes());
        }
    }

    private MiruFieldFilter viewClassesFilter() {
        return new MiruFieldFilter(MiruFieldName.VIEW_CLASS_NAME.getFieldName(), ImmutableList.of(
                "ContentVersionActivitySearchView",
                "CommentVersionActivitySearchView",
                "LikeActivitySearchView",
                "UserFollowActivitySearchView",
                "MembershipActivitySearchView",
                "PlaceActivitySearchView"));
    }

    private List<MiruFieldFilter> buildFieldFilters(boolean inbox, Id userId) {
        if (inbox) {
            return ImmutableList.of(new MiruFieldFilter(MiruFieldName.PARTICIPANT_IDS.getFieldName(),
                    ImmutableList.of(userId.toStringForm())));
        } else {
            int numUsers = random.nextInt(queryUsers);
            int numContainers = random.nextInt(queryContainers);
            return ImmutableList.of(
                    new MiruFieldFilter(MiruFieldName.CONTAINER_IDS.getFieldName(), ImmutableList.copyOf(
                            Lists.transform(featureSupplier.oldContainers(numContainers), ID_TO_TERMID))),
                    new MiruFieldFilter(MiruFieldName.AUTHOR_ID.getFieldName(), ImmutableList.copyOf(
                            Lists.transform(featureSupplier.oldUsers(numUsers), ID_TO_TERMID))));
        }
    }

    private MiruTimeRange buildTimeRange(boolean paging) {
        long last = featureSupplier.lastTimestamp();
        // 50% of timestamps in the latest 10% of activity
        OneTailedRandomNumber randomNumber = new OneTailedRandomNumber(2.0, 10, 0, last);
        long weightedRecentTimestamp = last - randomNumber.get(random);
        if (paging) {
            return new MiruTimeRange(0, weightedRecentTimestamp);
        } else {
            return new MiruTimeRange(weightedRecentTimestamp, Long.MAX_VALUE);
        }
    }

    private Optional<MiruFilter> buildConstraintsFilter(boolean inbox, Id userId) {
        ImmutableList.Builder<MiruFieldFilter> fieldFiltersBuilder = ImmutableList.builder();
        if (inbox) {
            if (random.nextInt(100) < 1) {
                // 1% filter for mentions
                fieldFiltersBuilder.add(new MiruFieldFilter(MiruFieldName.MENTIONED_USER_IDS.getFieldName(), ImmutableList.of(ID_TO_TERMID.apply(userId))));
            }
        } else {
            // activity filters?
        }

        List<MiruFieldFilter> fieldFilters = fieldFiltersBuilder.build();
        if (fieldFilters.isEmpty()) {
            return Optional.absent();
        } else {
            return Optional.of(new MiruFilter(
                    MiruFilterOperation.and,
                    Optional.of(fieldFilters),
                    Optional.<List<MiruFilter>>absent()));
        }
    }

    private static final Function<Id, String> ID_TO_TERMID = new Function<Id, String>() {
        @Nullable
        @Override
        public String apply(@Nullable Id input) {
            return input != null ? input.toStringForm() : null;
        }
    };
}
