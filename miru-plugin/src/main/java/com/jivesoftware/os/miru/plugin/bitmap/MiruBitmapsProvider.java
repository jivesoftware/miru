package com.jivesoftware.os.miru.plugin.bitmap;

import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public interface MiruBitmapsProvider {

    MiruBitmaps<?, ?> getBitmaps(MiruTenantId tenantnId);
}
