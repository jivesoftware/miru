package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.miru.api.base.MiruTenantId;

public interface BulkExport<T, C> {

    T bulkExport(MiruTenantId tenantId, C callback) throws Exception;

}
