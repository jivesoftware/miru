package com.jivesoftware.os.miru.stream.plugins.count;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

public class MiruDistinctCountQueryParams {

    private final MiruTenantId tenantId;
    private final MiruDistinctCountQueryCriteria queryCriteria;

    public MiruDistinctCountQueryParams(MiruTenantId tenantId,
            MiruDistinctCountQueryCriteria queryCriteria) {
        this.tenantId = tenantId;
        this.queryCriteria = queryCriteria;
    }

    @JsonCreator
    public static MiruDistinctCountQueryParams fromJson(
            @JsonProperty("tenantId") MiruTenantId tenantId,
            @JsonProperty("queryCriteria") MiruDistinctCountQueryCriteria queryCriteria) {
        return new MiruDistinctCountQueryParams(
                tenantId,
                queryCriteria);
    }

    public MiruTenantId getTenantId() {
        return tenantId;
    }

    public MiruDistinctCountQueryCriteria getQueryCriteria() {
        return queryCriteria;
    }
}
