package com.jivesoftware.os.miru.service.index.lab;

import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruRemovalIndex;
import com.jivesoftware.os.miru.plugin.partition.TrackError;

public class LabRemovalIndex<BM extends IBM, IBM> extends LabInvertedIndex<BM, IBM> implements MiruRemovalIndex<BM, IBM> {

    public LabRemovalIndex(OrderIdProvider idProvider,
        MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        boolean atomized,
        ValueIndex valueIndex,
        byte[] keyBytes,
        Object mutationLock,
        long labFieldDeltaMaxCardinality) {

        super(idProvider,
            bitmaps,
            trackError,
            "removal",
            -4,
            atomized,
            keyBytes,
            valueIndex,
            null,
            null,
            mutationLock,
            labFieldDeltaMaxCardinality);
    }
}
