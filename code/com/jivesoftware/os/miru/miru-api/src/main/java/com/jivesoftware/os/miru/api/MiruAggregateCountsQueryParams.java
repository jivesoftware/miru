package com.jivesoftware.os.miru.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;

public class MiruAggregateCountsQueryParams {

    private final MiruTenantId tenantId;
    private final Optional<MiruActorId> userIdentity;
    private final Optional<MiruAuthzExpression> authzExpression;
    private final MiruAggregateCountsQueryCriteria queryCriteria;

    public MiruAggregateCountsQueryParams(
        MiruTenantId tenantId,
        Optional<MiruActorId> userIdentity,
        Optional<MiruAuthzExpression> authzExpression,
        MiruAggregateCountsQueryCriteria queryCriteria) {
        this.tenantId = tenantId;
        this.userIdentity = userIdentity;
        this.authzExpression = authzExpression;
        this.queryCriteria = queryCriteria;
    }

    @JsonCreator
    public static MiruAggregateCountsQueryParams fromJson(
        @JsonProperty("tenantId") MiruTenantId tenantId,
        @JsonProperty("userIdentity") MiruActorId userIdentity,
        @JsonProperty("authzExpression") MiruAuthzExpression authzExpression,
        @JsonProperty("queryCriteria") MiruAggregateCountsQueryCriteria queryCriteria) {
        return new MiruAggregateCountsQueryParams(
            tenantId,
            Optional.fromNullable(userIdentity),
            Optional.fromNullable(authzExpression),
            queryCriteria);
    }

    public MiruTenantId getTenantId() {
        return tenantId;
    }

    public Optional<MiruActorId> getUserIdentity() {
        return userIdentity;
    }

    @JsonGetter("userIdentity")
    public MiruActorId getUserIdentityNullable() {
        return userIdentity.orNull();
    }

    public Optional<MiruAuthzExpression> getAuthzExpression() {
        return authzExpression;
    }

    @JsonGetter("authzExpression")
    public MiruAuthzExpression getAuthzNullable() {
        return authzExpression.orNull();
    }

    public MiruAggregateCountsQueryCriteria getQueryCriteria() {
        return queryCriteria;
    }
}
