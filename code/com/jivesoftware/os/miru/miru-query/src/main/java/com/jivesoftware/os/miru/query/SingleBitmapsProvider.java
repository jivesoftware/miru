package com.jivesoftware.os.miru.query;

import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public class SingleBitmapsProvider<BM> implements MiruBitmapsProvider {

    private final MiruBitmaps<BM> bitmaps;

    public SingleBitmapsProvider(MiruBitmaps<BM> bitmaps) {
        this.bitmaps = bitmaps;
    }

    @Override
    public MiruBitmaps<BM> getBitmaps(MiruTenantId tenantnId) {
        return bitmaps;
    }
}
