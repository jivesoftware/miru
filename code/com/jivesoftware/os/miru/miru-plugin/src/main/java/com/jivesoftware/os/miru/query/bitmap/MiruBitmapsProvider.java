package com.jivesoftware.os.miru.query.bitmap;

import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public interface MiruBitmapsProvider {

    MiruBitmaps<?> getBitmaps(MiruTenantId tenantnId);
}
