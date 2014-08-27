package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.miru.api.base.MiruTenantId;

public interface BulkImport<T> {

    void bulkImport(MiruTenantId tenantId, BulkExport<T> importItems) throws Exception;

}
