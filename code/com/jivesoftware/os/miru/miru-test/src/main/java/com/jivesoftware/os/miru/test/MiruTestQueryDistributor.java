package com.jivesoftware.os.miru.test;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jivesoftware.jive.platform.model.model.api.Ref;
import com.jivesoftware.jive.ui.base.model.event.User;
import com.jivesoftware.jive.ui.base.model.id.CompositeIds;
import com.jivesoftware.jive.ui.base.model.view.CommentVersionActivitySearchView;
import com.jivesoftware.jive.ui.base.model.view.ContentVersionActivitySearchView;
import com.jivesoftware.jive.ui.base.model.view.LikeActivitySearchView;
import com.jivesoftware.jive.ui.base.model.view.MembershipActivitySearchView;
import com.jivesoftware.jive.ui.base.model.view.PlaceActivitySearchView;
import com.jivesoftware.jive.ui.base.model.view.UserFollowActivitySearchView;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruAggregateCountsQueryCriteria;
import com.jivesoftware.os.miru.api.MiruAggregateCountsQueryParams;
import com.jivesoftware.os.miru.api.MiruDistinctCountQueryCriteria;
import com.jivesoftware.os.miru.api.MiruDistinctCountQueryParams;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
import com.jivesoftware.os.miru.api.query.MiruTimeRange;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import java.util.Random;
import javax.annotation.Nullable;

/**
 *
 */
public class MiruTestQueryDistributor {

    private final Random random;
    private final MiruTestFeatureSupplier featureSupplier;
    private final int numQueries;
    private final int queryUsers;
    private final int queryContainers;
    private final int numResultsAggregateCounts;
    private final int numResultsDistinctCount;

    private final OneTailedRandomNumber pageNumber = new OneTailedRandomNumber(2.0, 10, 0, 10); // 50% back one page, 0.1% back 10 pages

    public MiruTestQueryDistributor(Random random, MiruTestFeatureSupplier featureSupplier, int numQueries, int queryUsers, int queryContainers,
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

    public MiruAggregateCountsQueryParams aggregateCountsQuery(boolean inbox) {
        Id userId = featureSupplier.oldUsers(1).get(0);
        Ref<User> user = Ref.fromId(userId, User.class);
        MiruAggregateCountsQueryCriteria.Builder criteriaBuilder = new MiruAggregateCountsQueryCriteria.Builder()
            .setDesiredNumberOfDistincts(numResultsAggregateCounts + 1) // we usually add 1 for "hasMore"
            .setStreamId(streamId(inbox, user))
            .setStreamFilter(new MiruFilter(
                MiruFilterOperation.and,
                Optional.of(ImmutableList.of(viewClassesFilter())),
                Optional.of(ImmutableList.of(
                    new MiruFilter(
                        MiruFilterOperation.or,
                        Optional.of(buildFieldFilters(inbox, userId)),
                        Optional.<ImmutableList<MiruFilter>>absent())))));

        if (random.nextInt(100) < 10) {
            // 10% page, which uses an origin timestamp plus an offset
            criteriaBuilder.setAnswerTimeRange(buildTimeRange(true));
            criteriaBuilder.setStartFromDistinctN(numResultsAggregateCounts * (int) (1 + pageNumber.get(random)));
        }

        if (!inbox) {
            // activity stream applies count time range for zippers
            criteriaBuilder.setCountTimeRange(buildTimeRange(false));
        }

        Optional<MiruFilter> constraints = buildConstraintsFilter(inbox, userId);
        if (constraints.isPresent()) {
            criteriaBuilder.setConstraintsFilter(constraints.get());
        }

        return new MiruAggregateCountsQueryParams(
            featureSupplier.miruTenantId(),
            Optional.<MiruActorId>absent(),
            Optional.<MiruAuthzExpression>of(new MiruAuthzExpression(Lists.newArrayList(featureSupplier.userAuthz(userId)))),
            criteriaBuilder.build());
    }

    public MiruDistinctCountQueryParams distinctCountQuery(boolean inbox) {
        Id userId = featureSupplier.oldUsers(1).get(0);
        Ref<User> user = Ref.fromId(userId, User.class);
        MiruDistinctCountQueryCriteria.Builder criteriaBuilder = new MiruDistinctCountQueryCriteria.Builder()
            .setDesiredNumberOfDistincts(numResultsDistinctCount + 1) // we usually add 1 for "hasMore"
            .setStreamId(streamId(inbox, user))
            .setStreamFilter(new MiruFilter(
                MiruFilterOperation.and,
                Optional.of(ImmutableList.of(viewClassesFilter())),
                Optional.of(ImmutableList.of(
                    new MiruFilter(
                        MiruFilterOperation.or,
                        Optional.of(buildFieldFilters(inbox, userId)),
                        Optional.<ImmutableList<MiruFilter>>absent())))));

        if (!inbox) {
            // activity stream gets distinct count after last time viewed
            criteriaBuilder.setTimeRange(buildTimeRange(false));
        }

        Optional<MiruFilter> constraintsFilter = buildConstraintsFilter(inbox, userId);
        if (constraintsFilter.isPresent()) {
            criteriaBuilder.setConstraintsFilter(constraintsFilter.get());
        }

        return new MiruDistinctCountQueryParams(
            featureSupplier.miruTenantId(),
            Optional.<MiruActorId>absent(),
            Optional.<MiruAuthzExpression>of(new MiruAuthzExpression(Lists.newArrayList(featureSupplier.userAuthz(userId)))),
            criteriaBuilder.build());
    }

    private Id streamId(boolean inbox, Ref<User> user) {
        if (inbox) {
            return CompositeIds.inboxStreamId(featureSupplier.tenantId(), user);
        } else {
            return CompositeIds.connectionsStreamId(featureSupplier.tenantId(), user);
        }
    }

    private MiruFieldFilter viewClassesFilter() {
        return new MiruFieldFilter(MiruFieldName.VIEW_CLASS_NAME.getFieldName(), ImmutableList.copyOf(Lists.transform(
            Lists.<Class<?>>newArrayList(
                ContentVersionActivitySearchView.class,
                CommentVersionActivitySearchView.class,
                LikeActivitySearchView.class,
                UserFollowActivitySearchView.class,
                MembershipActivitySearchView.class,
                PlaceActivitySearchView.class),
            CLASS_NAME_TO_TERMID)));
    }

    private ImmutableList<MiruFieldFilter> buildFieldFilters(boolean inbox, Id userId) {
        if (inbox) {
            return ImmutableList.of(new MiruFieldFilter(MiruFieldName.PARTICIPANT_IDS.getFieldName(),
                ImmutableList.of(ID_TO_TERMID.apply(userId))));
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

        ImmutableList<MiruFieldFilter> fieldFilters = fieldFiltersBuilder.build();
        if (fieldFilters.isEmpty()) {
            return Optional.absent();
        } else {
            return Optional.of(new MiruFilter(
                MiruFilterOperation.and,
                Optional.of(fieldFilters),
                Optional.<ImmutableList<MiruFilter>>absent()));
        }
    }

    private static final Function<Class<?>, MiruTermId> CLASS_NAME_TO_TERMID = new Function<Class<?>, MiruTermId>() {
        @Nullable
        @Override
        public MiruTermId apply(@Nullable Class<?> input) {
            return input != null ? new MiruTermId(input.getSimpleName().getBytes(Charsets.UTF_8)) : null;
        }
    };

    private static final Function<Id, MiruTermId> ID_TO_TERMID = new Function<Id, MiruTermId>() {
        @Nullable
        @Override
        public MiruTermId apply(@Nullable Id input) {
            return input != null ? new MiruTermId(input.toStringForm().getBytes(Charsets.UTF_8)) : null;
        }
    };
}
