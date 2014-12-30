package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.miru.api.base.MiruTenantId;

public interface BulkImport<T, C> {

    void bulkImport(MiruTenantId tenantId, BulkExport<T, C> export) throws Exception;

}
