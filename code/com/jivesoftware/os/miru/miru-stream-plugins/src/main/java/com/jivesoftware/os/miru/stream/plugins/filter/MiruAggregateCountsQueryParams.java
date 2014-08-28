package com.jivesoftware.os.miru.stream.plugins.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

public class MiruAggregateCountsQueryParams {

    private final MiruTenantId tenantId;
    private final MiruAggregateCountsQueryCriteria queryCriteria;

    public MiruAggregateCountsQueryParams(
            MiruTenantId tenantId,
            MiruAggregateCountsQueryCriteria queryCriteria) {
        this.tenantId = tenantId;
        this.queryCriteria = queryCriteria;
    }

    @JsonCreator
    public static MiruAggregateCountsQueryParams fromJson(
            @JsonProperty("tenantId") MiruTenantId tenantId,
            @JsonProperty("queryCriteria") MiruAggregateCountsQueryCriteria queryCriteria) {
        return new MiruAggregateCountsQueryParams(
                tenantId,
                queryCriteria);
    }

    public MiruTenantId getTenantId() {
        return tenantId;
    }


    public MiruAggregateCountsQueryCriteria getQueryCriteria() {
        return queryCriteria;
    }
}
