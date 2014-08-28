package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

public class MiruTrendingQueryParams {

    private final MiruTenantId tenantId;
    private final MiruTrendingQueryCriteria queryCriteria;

    public MiruTrendingQueryParams(MiruTenantId tenantId,
            MiruTrendingQueryCriteria queryCriteria) {
        this.tenantId = tenantId;
        this.queryCriteria = queryCriteria;
    }

    @JsonCreator
    public static MiruTrendingQueryParams fromJson(
            @JsonProperty("tenantId") MiruTenantId tenantId,
            @JsonProperty("queryCriteria") MiruTrendingQueryCriteria queryCriteria) {
        return new MiruTrendingQueryParams(
                tenantId,
                queryCriteria);
    }

    public MiruTenantId getTenantId() {
        return tenantId;
    }

    public MiruTrendingQueryCriteria getQueryCriteria() {
        return queryCriteria;
    }
}
