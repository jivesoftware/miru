package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.plugin.index.MiruUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.SimpleBulkExport;
import java.io.IOException;
import java.util.Collections;

/** @author jonathan */
public class MiruInMemoryUnreadTrackingIndex<BM> implements MiruUnreadTrackingIndex<BM>,
    BulkImport<Void, KeyedInvertedIndexStream<BM>>,
    BulkExport<Void, KeyedInvertedIndexStream<BM>> {

    private final MiruBitmaps<BM> bitmaps;
    private final KeyValueStore<byte[], ReadWrite<BM>> store;

    public MiruInMemoryUnreadTrackingIndex(MiruBitmaps<BM> bitmaps,
        KeyValueStore<byte[], ReadWrite<BM>> store) {
        this.bitmaps = bitmaps;
        this.store = store;
    }

    @Override
    public void index(MiruStreamId streamId, int id) throws Exception {
        getAppender(streamId).append(id);
    }

    @Override
    public Optional<BM> getUnread(MiruStreamId streamId) throws Exception {
        return getIndex(streamId).getIndex();
    }

    @Override
    public MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception {
        return getIndex(streamId);
    }

    private MiruInMemoryInvertedIndex<BM> getIndex(MiruStreamId streamId) throws Exception {
        return new MiruInMemoryInvertedIndex<>(bitmaps, store, streamId.getBytes(), -1);
    }

    @Override
    public void applyRead(MiruStreamId streamId, BM readMask) throws Exception {
        MiruInvertedIndex<BM> unread = getIndex(streamId);
        unread.andNotToSourceSize(Collections.singletonList(readMask));
    }

    @Override
    public void applyUnread(MiruStreamId streamId, BM unreadMask) throws Exception {
        MiruInvertedIndex<BM> unread = getIndex(streamId);
        unread.orToSourceSize(unreadMask);
    }

    @Override
    public void close() {
    }

    @Override
    public Void bulkExport(MiruTenantId tenantId, final KeyedInvertedIndexStream<BM> callback) throws Exception {
        store.streamKeys(new KeyValueStore.KeyStream<byte[]>() {
            @Override
            public boolean stream(byte[] bytes) throws IOException {
                try {
                    MiruInvertedIndex<BM> index = getIndex(new MiruStreamId(bytes));
                    return callback.stream(bytes, index);
                } catch (Exception e) {
                    throw new IOException("Failed to stream export", e);
                }
            }
        });
        return null;
    }

    @Override
    public void bulkImport(final MiruTenantId tenantId, BulkExport<Void, KeyedInvertedIndexStream<BM>> export) throws Exception {
        export.bulkExport(tenantId, new KeyedInvertedIndexStream<BM>() {
            @Override
            public boolean stream(byte[] key, MiruInvertedIndex<BM> importIndex) throws IOException {
                try {
                    MiruInMemoryInvertedIndex<BM> invertedIndex = getIndex(new MiruStreamId(key));
                    invertedIndex.bulkImport(tenantId, new SimpleBulkExport<>(importIndex));
                    return true;
                } catch (Exception e) {
                    throw new IOException("Failed to stream import", e);
                }
            }
        });
    }
}
