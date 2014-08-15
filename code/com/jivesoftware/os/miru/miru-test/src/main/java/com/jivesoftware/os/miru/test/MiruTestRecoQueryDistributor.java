package com.jivesoftware.os.miru.test;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.miru.api.*;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;

import javax.annotation.Nullable;

/**
 *
 */
public class MiruTestRecoQueryDistributor {

    private final int numQueries;
    private final MiruTestFeatureSupplier featureSupplier;
    private final int numResultsGlobalTrendy;
    private final int numResultsCollaborativeFiltering;

    public MiruTestRecoQueryDistributor(int numQueries, MiruTestFeatureSupplier featureSupplier, int numResultsGlobalTrendy, int
            numResultsCollaborativeFiltering) {
        this.numQueries = numQueries;
        this.featureSupplier = featureSupplier;
        this.numResultsGlobalTrendy = numResultsGlobalTrendy;
        this.numResultsCollaborativeFiltering = numResultsCollaborativeFiltering;
    }

    public int getNumQueries() {
        return numQueries;
    }

    public MiruTrendingQueryParams globalTrending() {
        Id userId = featureSupplier.oldUsers(1).get(0);
        MiruTrendingQueryCriteria.Builder criteriaBuilder = new MiruTrendingQueryCriteria.Builder()
                .setDesiredNumberOfDistincts(numResultsGlobalTrendy)
                .setConstraintsFilter(new MiruFilter(
                        MiruFilterOperation.or,
                        Optional.of(ImmutableList.of(viewClassesFilter())),
                        Optional.<ImmutableList<MiruFilter>>absent()));

        return new MiruTrendingQueryParams(
                featureSupplier.miruTenantId(),
                Optional.<MiruActorId>absent(),
                Optional.of(new MiruAuthzExpression(Lists.newArrayList(featureSupplier.userAuthz(userId)))),
                criteriaBuilder.build());
    }

    public MiruRecoQueryParams collaborativeFiltering() {
        Id userId = featureSupplier.oldUsers(1).get(0);
        MiruFieldFilter miruFieldFilter = new MiruFieldFilter(
                MiruFieldName.AUTHOR_ID.getFieldName(),
                ImmutableList.of(new MiruTermId(userId.toStringForm().getBytes(Charsets.UTF_8))));
        MiruFilter filter = new MiruFilter(MiruFilterOperation.or, Optional.of(ImmutableList.of(miruFieldFilter)),
                Optional.<ImmutableList<MiruFilter>>absent());
        MiruRecoQueryCriteria criteria = new MiruRecoQueryCriteria(
                filter,
                MiruFieldName.ACTIVITY_PARENT.getFieldName(),
                MiruFieldName.ACTIVITY_PARENT.getFieldName(),
                MiruFieldName.ACTIVITY_PARENT.getFieldName(),
                MiruFieldName.AUTHOR_ID.getFieldName(),
                MiruFieldName.AUTHOR_ID.getFieldName(),
                MiruFieldName.AUTHOR_ID.getFieldName(),
                MiruFieldName.ACTIVITY_PARENT.getFieldName(),
                MiruFieldName.ACTIVITY_PARENT.getFieldName(),
                numResultsCollaborativeFiltering);

        return new MiruRecoQueryParams(
                featureSupplier.miruTenantId(),
                Optional.<MiruActorId>absent(),
                Optional.of(new MiruAuthzExpression(Lists.newArrayList(featureSupplier.userAuthz(userId)))),
                criteria);
    }

    private MiruFieldFilter viewClassesFilter() {
        return new MiruFieldFilter(MiruFieldName.VIEW_CLASS_NAME.getFieldName(), ImmutableList.copyOf(Lists.transform(
                Lists.newArrayList(
                        "ContentVersionActivitySearchView",
                        "CommentVersionActivitySearchView",
                        "LikeActivitySearchView",
                        "UserFollowActivitySearchView",
                        "MembershipActivitySearchView",
                        "PlaceActivitySearchView"),
                CLASS_NAME_TO_TERMID)));
    }

    private static final Function<String, MiruTermId> CLASS_NAME_TO_TERMID = new Function<String, MiruTermId>() {
        @Nullable
        @Override
        public MiruTermId apply(@Nullable String input) {
            return input != null ? new MiruTermId(input.getBytes(Charsets.UTF_8)) : null;
        }
    };
}
