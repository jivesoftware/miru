package com.jivesoftware.os.miru.plugin.bitmap;

import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public class SingleBitmapsProvider implements MiruBitmapsProvider {

    private final MiruBitmaps<?, ?> bitmaps;

    public SingleBitmapsProvider(MiruBitmaps<?, ?> bitmaps) {
        this.bitmaps = bitmaps;
    }

    @Override
    public <BM extends IBM, IBM> MiruBitmaps<BM, IBM> getBitmaps(MiruTenantId tenantnId) {
        return (MiruBitmaps) bitmaps;
    }
}
