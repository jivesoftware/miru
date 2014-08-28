package com.jivesoftware.os.miru.query;

import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public interface MiruBitmapsProvider {

    MiruBitmaps<?> getBitmaps(MiruTenantId tenantnId);
}
