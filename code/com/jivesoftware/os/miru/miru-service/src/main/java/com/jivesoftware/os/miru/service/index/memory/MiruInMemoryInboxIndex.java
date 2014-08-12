package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruInboxIndex;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryInboxIndex.InboxAndLastActivityIndex;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * An in-memory representation of all inbox indexes in a given system
 */
public class MiruInMemoryInboxIndex implements MiruInboxIndex, BulkImport<InboxAndLastActivityIndex>, BulkExport<InboxAndLastActivityIndex> {

    private final ConcurrentMap<MiruStreamId, MiruInvertedIndex> index;
    private final Map<MiruStreamId, Integer> lastActivityIndex;

    public MiruInMemoryInboxIndex() {
        this.index = new ConcurrentHashMap<MiruStreamId, MiruInvertedIndex>();
        this.lastActivityIndex = new ConcurrentHashMap<MiruStreamId, Integer>();
    }

    @Override
    public void index(MiruStreamId streamId, int id) throws Exception {
        MiruInvertedIndexAppender inbox = getAppender(streamId);
        inbox.append(id);
    }

    @Override
    public Optional<EWAHCompressedBitmap> getInbox(MiruStreamId streamId) throws Exception {
        MiruInvertedIndex got = index.get(streamId);
        if (got == null) {
            return Optional.absent();
        }
        return Optional.<EWAHCompressedBitmap>fromNullable(got.getIndex());
    }

    @Override
    public MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception {
        MiruInvertedIndex got = index.get(streamId);
        if (got == null) {
            index.putIfAbsent(streamId, new MiruInMemoryInvertedIndex(new EWAHCompressedBitmap()));
            got = index.get(streamId);
        }
        return got;
    }

    @Override
    public int getLastActivityIndex(MiruStreamId streamId) {
        Integer got = lastActivityIndex.get(streamId);
        if (got == null) {
            got = -1;
        }
        return got;
    }

    @Override
    public void setLastActivityIndex(MiruStreamId streamId, int activityIndex) {
        lastActivityIndex.put(streamId, activityIndex);
    }

    @Override
    public long sizeInMemory() throws Exception {
        long sizeInBytes = lastActivityIndex.size() * 12; // 8 byte stream ID + 4 byte index
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
    public InboxAndLastActivityIndex bulkExport() throws Exception {
        return new InboxAndLastActivityIndex(index, lastActivityIndex);
    }

    @Override
    public void bulkImport(BulkExport<InboxAndLastActivityIndex> importItems) throws Exception {
        InboxAndLastActivityIndex inboxAndLastActivityIndex = importItems.bulkExport();
        this.index.putAll(inboxAndLastActivityIndex.index);
        this.lastActivityIndex.putAll(inboxAndLastActivityIndex.lastActivityIndex);
    }

    public static class InboxAndLastActivityIndex {
        public final Map<MiruStreamId, MiruInvertedIndex> index;
        public final Map<MiruStreamId, Integer> lastActivityIndex;

        public InboxAndLastActivityIndex(Map<MiruStreamId, MiruInvertedIndex> index, Map<MiruStreamId, Integer> lastActivityIndex) {
            this.index = ImmutableMap.copyOf(index);
            this.lastActivityIndex = ImmutableMap.copyOf(lastActivityIndex);
        }
    }
}
