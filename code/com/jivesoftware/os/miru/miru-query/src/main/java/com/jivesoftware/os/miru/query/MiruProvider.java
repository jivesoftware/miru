package com.jivesoftware.os.miru.query;

import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public interface MiruProvider<T extends Miru> {

    T getMiru(MiruTenantId tenantId);

    MiruActivityInternExtern getActivityInternExtern(MiruTenantId tenantId);

    MiruJustInTimeBackfillerizer getBackfillerizer(MiruTenantId tenantId);

}
