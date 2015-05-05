package com.jivesoftware.os.miru.plugin.solution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import java.io.Serializable;

/**
 * @param <Q> Query
 * @author jonathan.colt
 */
public class MiruRequest<Q> implements Serializable {

    public final MiruTenantId tenantId;
    public final MiruActorId actorId;
    public final MiruAuthzExpression authzExpression;
    public final Q query;
    public final MiruSolutionLogLevel logLevel;

    @JsonCreator
    public MiruRequest(@JsonProperty("tenantId") MiruTenantId tenantId,
        @JsonProperty("actorId") MiruActorId actorId,
        @JsonProperty("authzExpression") MiruAuthzExpression authzExpression,
        @JsonProperty("query") Q query,
        @JsonProperty("logLevel") MiruSolutionLogLevel logLevel) {
        this.tenantId = tenantId;
        this.actorId = actorId;
        this.query = query;
        this.authzExpression = authzExpression;
        this.logLevel = logLevel;
    }

    @Override
    public String toString() {
        return "MiruRequest{"
            + "tenantId=" + tenantId
            + ", actorId=" + actorId
            + ", authzExpression=" + authzExpression
            + ", query=" + query
            + ", logLevel=" + logLevel
            + '}';
    }

}
