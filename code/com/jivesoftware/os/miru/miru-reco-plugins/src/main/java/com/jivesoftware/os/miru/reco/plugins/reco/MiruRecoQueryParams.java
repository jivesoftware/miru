package com.jivesoftware.os.miru.reco.plugins.reco;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

public class MiruRecoQueryParams {

    private final MiruTenantId tenantId;
    private final MiruRecoQueryCriteria queryCriteria;

    public MiruRecoQueryParams(MiruTenantId tenantId, MiruRecoQueryCriteria queryCriteria) {
        this.tenantId = tenantId;
        this.queryCriteria = queryCriteria;
    }

    @JsonCreator
    public static MiruRecoQueryParams fromJson(
            @JsonProperty("tenantId") MiruTenantId tenantId,
            @JsonProperty("queryCriteria") MiruRecoQueryCriteria queryCriteria) {
        return new MiruRecoQueryParams(
                tenantId,
                queryCriteria);
    }

    public MiruTenantId getTenantId() {
        return tenantId;
    }

    public MiruRecoQueryCriteria getQueryCriteria() {
        return queryCriteria;
    }
}
