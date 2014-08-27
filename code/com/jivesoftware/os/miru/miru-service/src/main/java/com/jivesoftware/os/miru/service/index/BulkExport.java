package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.miru.api.base.MiruTenantId;

public interface BulkExport<T> {

    T bulkExport(MiruTenantId tenantId) throws Exception;

}
