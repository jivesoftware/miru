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
    public MiruBitmaps<?, ?> getBitmaps(MiruTenantId tenantnId) {
        return bitmaps;
    }
}
