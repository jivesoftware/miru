package com.jivesoftware.os.miru.service.index.lab;

import com.google.common.primitives.Bytes;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.plugin.index.MiruUnreadTrackingIndex;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import java.util.Collections;

/** @author jonathan */
public class LabUnreadTrackingIndex<BM extends IBM, IBM> implements MiruUnreadTrackingIndex<BM, IBM> {

    private final OrderIdProvider idProvider;
    private final MiruBitmaps<BM, IBM> bitmaps;
    private final TrackError trackError;
    private final byte[] bitmapPrefix;
    private final byte[] lastActivityIndexPrefix;
    private final boolean atomized;
    private final ValueIndex<byte[]>[] stores;
    private final StripingLocksProvider<MiruStreamId> stripingLocksProvider;

    public LabUnreadTrackingIndex(OrderIdProvider idProvider,
        MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        byte[] bitmapPrefix,
        byte[] lastActivityIndexPrefix,
        boolean atomized,
        ValueIndex<byte[]>[] stores,
        StripingLocksProvider<MiruStreamId> stripingLocksProvider)
        throws Exception {

        this.idProvider = idProvider;
        this.bitmaps = bitmaps;
        this.trackError = trackError;
        this.bitmapPrefix = bitmapPrefix;
        this.lastActivityIndexPrefix = lastActivityIndexPrefix;
        this.atomized = atomized;
        this.stores = stores;
        this.stripingLocksProvider = stripingLocksProvider;
    }

    private ValueIndex<byte[]> getStore(MiruStreamId streamId) {
        return stores[Math.abs(streamId.hashCode() % stores.length)];
    }

    @Override
    public void append(MiruStreamId streamId, StackBuffer stackBuffer, int... ids) throws Exception {
        getAppender(streamId).set(stackBuffer, ids);
    }

    @Override
    public MiruInvertedIndex<BM, IBM> getUnread(MiruStreamId streamId) throws Exception {
        return new LabInvertedIndex<>(idProvider,
            bitmaps,
            trackError,
            "unread",
            -1,
            atomized,
            storeKey(bitmapPrefix, streamId.getBytes()),
            getStore(streamId),
            null,
            null,
            stripingLocksProvider.lock(streamId, 0));
    }

    private byte[] storeKey(byte[] prefix, byte[] streamIdBytes) {
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
        return getUnread(streamId);
    }

    @Override
    public void applyRead(MiruStreamId streamId, IBM readMask, StackBuffer stackBuffer) throws Exception {
        MiruInvertedIndex<BM, IBM> unread = getUnread(streamId);
        unread.andNotToSourceSize(Collections.singletonList(readMask), stackBuffer);
    }

    @Override
    public void applyUnread(MiruStreamId streamId, IBM unreadMask, StackBuffer stackBuffer) throws Exception {
        MiruInvertedIndex<BM, IBM> unread = getUnread(streamId);
        unread.orToSourceSize(unreadMask, stackBuffer);
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public void setLastActivityIndex(MiruStreamId streamId, int lastActivityIndex, StackBuffer stackBuffer) throws Exception {
        ValueIndex<byte[]> store = getStore(streamId);
        byte[] key = storeKey(lastActivityIndexPrefix, streamId.getBytes());
        long timestamp = System.currentTimeMillis();
        long version = idProvider.nextId();
        store.append(
            stream -> {
                return stream.stream(0, key, timestamp, false, version, UIO.intBytes(lastActivityIndex, new byte[4], 0));
            },
            false,
            new BolBuffer(),
            new BolBuffer());
    }

    @Override
    public int getLastActivityIndex(MiruStreamId streamId, StackBuffer stackBuffer) throws Exception {
        ValueIndex<byte[]> store = getStore(streamId);
        byte[] key = storeKey(lastActivityIndexPrefix, streamId.getBytes());
        int[] result = { -1 };
        store.get(keyStream -> keyStream.key(0, key, 0, key.length),
            (index, key1, timestamp, tombstoned, version, payload) -> {
                if (!tombstoned && payload != null) {
                    result[0] = payload.getInt(0);
                }
                return true;
            },
            true);

        if (result[0] == -1) {
            result[0] = getUnread(streamId).lastId(stackBuffer);
        }
        return result[0];
    }
}
