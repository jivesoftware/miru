package com.jivesoftware.os.miru.plugin.bitmap;

import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public interface MiruBitmapsProvider {

    <BM extends IBM, IBM> MiruBitmaps<BM, IBM> getBitmaps(MiruTenantId tenantnId);
}
