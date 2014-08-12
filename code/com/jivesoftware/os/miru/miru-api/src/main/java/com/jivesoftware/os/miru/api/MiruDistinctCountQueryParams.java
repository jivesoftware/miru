package com.jivesoftware.os.miru.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;

public class MiruDistinctCountQueryParams {

    private final MiruTenantId tenantId;
    private final Optional<MiruActorId> userIdentity;
    private final Optional<MiruAuthzExpression> authzExpression;
    private final MiruDistinctCountQueryCriteria queryCriteria;

    public MiruDistinctCountQueryParams(MiruTenantId tenantId, Optional<MiruActorId> userIdentity,
        Optional<MiruAuthzExpression> authzExpression, MiruDistinctCountQueryCriteria queryCriteria) {
        this.tenantId = tenantId;
        this.userIdentity = userIdentity;
        this.authzExpression = authzExpression;
        this.queryCriteria = queryCriteria;
    }

    @JsonCreator
    public static MiruDistinctCountQueryParams fromJson(
        @JsonProperty("tenantId") MiruTenantId tenantId,
        @JsonProperty("userIdentity") MiruActorId userIdentity,
        @JsonProperty("authzExpression") MiruAuthzExpression authzExpression,
        @JsonProperty("queryCriteria") MiruDistinctCountQueryCriteria queryCriteria) {
        return new MiruDistinctCountQueryParams(
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
    public MiruAuthzExpression getAuthzExpressionNullable() {
        return authzExpression.orNull();
    }

    public MiruDistinctCountQueryCriteria getQueryCriteria() {
        return queryCriteria;
    }
}
