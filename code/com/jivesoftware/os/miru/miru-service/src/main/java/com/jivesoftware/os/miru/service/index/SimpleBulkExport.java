package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public class SimpleBulkExport<T> implements BulkExport<T, Void> {

    private final T payload;

    public SimpleBulkExport(T payload) {
        this.payload = payload;
    }

    @Override
    public T bulkExport(MiruTenantId tenantId, Void callback) throws Exception {
        return payload;
    }
}
