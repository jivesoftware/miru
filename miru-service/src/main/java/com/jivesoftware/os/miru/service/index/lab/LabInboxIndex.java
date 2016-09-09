package com.jivesoftware.os.miru.service.index.lab;

import com.google.common.primitives.Bytes;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInboxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.plugin.partition.TrackError;

/** @author jonathan */
public class LabInboxIndex<BM extends IBM, IBM> implements MiruInboxIndex<BM, IBM> {

    private final OrderIdProvider idProvider;
    private final MiruBitmaps<BM, IBM> bitmaps;
    private final TrackError trackError;
    private final byte[] prefix;
    private final boolean atomized;
    private final ValueIndex[] stores;
    private final StripingLocksProvider<MiruStreamId> stripingLocksProvider;
    private final long labFieldDeltaMaxCardinality;

    public LabInboxIndex(OrderIdProvider idProvider,
        MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        byte[] prefix,
        boolean atomized,
        ValueIndex[] stores,
        StripingLocksProvider<MiruStreamId> stripingLocksProvider,
        long labFieldDeltaMaxCardinality)
        throws Exception {

        this.idProvider = idProvider;
        this.bitmaps = bitmaps;
        this.trackError = trackError;
        this.prefix = prefix;
        this.atomized = atomized;
        this.stores = stores;
        this.stripingLocksProvider = stripingLocksProvider;
        this.labFieldDeltaMaxCardinality = labFieldDeltaMaxCardinality;
    }

    private ValueIndex getStore(MiruStreamId streamId) {
        return stores[Math.abs(streamId.hashCode() % stores.length)];
    }

    @Override
    public void append(MiruStreamId streamId, StackBuffer stackBuffer, int... ids) throws Exception {
        getAppender(streamId).set(stackBuffer, ids);
    }

    @Override
    public MiruInvertedIndex<BM, IBM> getInbox(MiruStreamId streamId) {
        return new LabInvertedIndex<>(idProvider,
            bitmaps,
            trackError,
            "inbox",
            -2,
            atomized,
            bitmapIndexKey(streamId.getBytes()),
            getStore(streamId),
            null,
            null,
            stripingLocksProvider.lock(streamId, 0),
            labFieldDeltaMaxCardinality);
    }

    private byte[] bitmapIndexKey(byte[] streamIdBytes) {
        if (atomized) {
            byte[] streamIdLength = new byte[2];
            UIO.shortBytes((short) (streamIdBytes.length & 0xFFFF), streamIdLength, 0);
            return Bytes.concat(prefix, streamIdLength, streamIdBytes);
        } else {
            return Bytes.concat(prefix, streamIdBytes);
        }
    }

    @Override
    public MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception {
        return getInbox(streamId);
    }

    @Override
    public int getLastActivityIndex(MiruStreamId streamId, StackBuffer stackBuffer) throws Exception {
        return getInbox(streamId).lastId(stackBuffer);
    }

    @Override
    public void close() throws Exception {
    }
}
