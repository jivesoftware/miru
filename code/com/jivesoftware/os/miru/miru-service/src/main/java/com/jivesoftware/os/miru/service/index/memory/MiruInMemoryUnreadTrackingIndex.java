package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.query.MiruBitmaps;
import com.jivesoftware.os.miru.query.MiruInvertedIndex;
import com.jivesoftware.os.miru.query.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.query.MiruUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** @author jonathan */
public class MiruInMemoryUnreadTrackingIndex<BM> implements MiruUnreadTrackingIndex<BM>,
    BulkImport<Map<MiruStreamId, MiruInvertedIndex<BM>>>, BulkExport<Map<MiruStreamId, MiruInvertedIndex<BM>>> {

    private final MiruBitmaps<BM> bitmaps;
    private final ConcurrentMap<MiruStreamId, MiruInvertedIndex<BM>> index;

    public MiruInMemoryUnreadTrackingIndex(MiruBitmaps<BM> bitmaps) {
        this.bitmaps = bitmaps;
        this.index = new ConcurrentHashMap<>();
    }

    @Override
    public void index(MiruStreamId streamId, int id) throws Exception {
        getAppender(streamId).append(id);
    }

    @Override
    public Optional<BM> getUnread(MiruStreamId streamId) throws Exception {
        MiruInvertedIndex<BM> got = index.get(streamId);
        if (got == null) {
            return Optional.absent();
        }
        return Optional.<BM>fromNullable(got.getIndex());
    }

    @Override
    public MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception {
        return getOrCreateUnread(streamId);
    }

    private MiruInvertedIndex<BM> getOrCreateUnread(MiruStreamId streamId) throws Exception {
        MiruInvertedIndex<BM> got = index.get(streamId);
        if (got == null) {
            index.putIfAbsent(streamId, new MiruInMemoryInvertedIndex<>(bitmaps));
            got = index.get(streamId);
        }
        return got;
    }

    @Override
    public void applyRead(MiruStreamId streamId, BM readMask) throws Exception {
        MiruInvertedIndex<BM> unread = getOrCreateUnread(streamId);
        unread.andNotToSourceSize(readMask);
    }

    @Override
    public void applyUnread(MiruStreamId streamId, BM unreadMask) throws Exception {
        MiruInvertedIndex<BM> unread = getOrCreateUnread(streamId);
        unread.orToSourceSize(unreadMask);
    }

    @Override
    public long sizeInMemory() throws Exception {
        long sizeInBytes = 0;
        for (Map.Entry<MiruStreamId, MiruInvertedIndex<BM>> entry : index.entrySet()) {
            sizeInBytes += entry.getKey().getBytes().length + entry.getValue().sizeInMemory();
        }
        return sizeInBytes;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        long sizeInBytes = 0;
        for (Map.Entry<MiruStreamId, MiruInvertedIndex<BM>> entry : index.entrySet()) {
            sizeInBytes += entry.getValue().sizeOnDisk();
        }
        return sizeInBytes;
    }

    @Override
    public void close() {
    }

    @Override
    public Map<MiruStreamId, MiruInvertedIndex<BM>> bulkExport(MiruTenantId tenantId) throws Exception {
        return ImmutableMap.copyOf(index);
    }

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<Map<MiruStreamId, MiruInvertedIndex<BM>>> importItems) throws Exception {
        this.index.putAll(importItems.bulkExport(tenantId));
    }
}
