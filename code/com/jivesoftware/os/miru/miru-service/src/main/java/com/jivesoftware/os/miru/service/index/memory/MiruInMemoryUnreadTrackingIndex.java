package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.service.index.MiruUnreadTrackingIndex;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** @author jonathan */
public class MiruInMemoryUnreadTrackingIndex implements MiruUnreadTrackingIndex,
    BulkImport<Map<MiruStreamId, MiruInvertedIndex>>, BulkExport<Map<MiruStreamId, MiruInvertedIndex>> {

    private final ConcurrentMap<MiruStreamId, MiruInvertedIndex> index;

    public MiruInMemoryUnreadTrackingIndex() {
        this.index = new ConcurrentHashMap<>();
    }

    @Override
    public void index(MiruStreamId streamId, int id) throws Exception {
        getAppender(streamId).append(id);
    }

    @Override
    public Optional<EWAHCompressedBitmap> getUnread(MiruStreamId streamId) throws Exception {
        MiruInvertedIndex got = index.get(streamId);
        if (got == null) {
            return Optional.absent();
        }
        return Optional.<EWAHCompressedBitmap>fromNullable(got.getIndex());
    }

    @Override
    public MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception {
        return getOrCreateUnread(streamId);
    }

    private MiruInvertedIndex getOrCreateUnread(MiruStreamId streamId) throws Exception {
        MiruInvertedIndex got = index.get(streamId);
        if (got == null) {
            index.putIfAbsent(streamId, new MiruInMemoryInvertedIndex(new EWAHCompressedBitmap()));
            got = index.get(streamId);
        }
        return got;
    }

    @Override
    public void applyRead(MiruStreamId streamId, EWAHCompressedBitmap readMask) throws Exception {
        MiruInvertedIndex unread = getOrCreateUnread(streamId);
        unread.andNotToSourceSize(readMask);
    }

    @Override
    public void applyUnread(MiruStreamId streamId, EWAHCompressedBitmap unreadMask) throws Exception {
        MiruInvertedIndex unread = getOrCreateUnread(streamId);
        unread.orToSourceSize(unreadMask);
    }

    @Override
    public long sizeInMemory() throws Exception {
        long sizeInBytes = 0;
        for (Map.Entry<MiruStreamId, MiruInvertedIndex> entry : index.entrySet()) {
            sizeInBytes += entry.getKey().getBytes().length + entry.getValue().sizeInMemory();
        }
        return sizeInBytes;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        long sizeInBytes = 0;
        for (Map.Entry<MiruStreamId, MiruInvertedIndex> entry : index.entrySet()) {
            sizeInBytes += entry.getValue().sizeOnDisk();
        }
        return sizeInBytes;
    }

    @Override
    public void close() {
    }

    @Override
    public Map<MiruStreamId, MiruInvertedIndex> bulkExport() throws Exception {
        return ImmutableMap.copyOf(index);
    }

    @Override
    public void bulkImport(BulkExport<Map<MiruStreamId, MiruInvertedIndex>> importItems) throws Exception {
        this.index.putAll(importItems.bulkExport());
    }
}
