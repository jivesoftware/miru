package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInboxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruRemovalIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.index.MiruUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.locator.MiruResourcePartitionIdentifier;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;

/**
 * Composes the building blocks of a MiruContext together for convenience.
 *
 * @author jonathan
 */
public class MiruContext<BM> implements MiruRequestContext<BM> {

    public final MiruSchema schema;
    public final MiruTimeIndex timeIndex;
    public final MiruActivityIndex activityIndex;
    public final MiruFieldIndex<BM> fieldIndex;
    public final MiruAuthzIndex<BM> authzIndex;
    public final MiruRemovalIndex<BM> removalIndex;
    public final MiruUnreadTrackingIndex<BM> unreadTrackingIndex;
    public final MiruInboxIndex<BM> inboxIndex;
    public final MiruReadTrackingWALReader readTrackingWALReader;
    public final MiruActivityInternExtern activityInternExtern;
    public final StripingLocksProvider<MiruStreamId> streamLocks;
    public final Optional<MultiChunkStore> chunkStore;
    public final Optional<? extends MiruResourcePartitionIdentifier> transientResource;

    public MiruContext(MiruSchema schema,
        MiruTimeIndex timeIndex,
        MiruActivityIndex activityIndex,
        MiruFieldIndex<BM> fieldIndex,
        MiruAuthzIndex<BM> authzIndex,
        MiruRemovalIndex<BM> removalIndex,
        MiruUnreadTrackingIndex<BM> unreadTrackingIndex,
        MiruInboxIndex<BM> inboxIndex,
        MiruReadTrackingWALReader readTrackingWALReader,
        MiruActivityInternExtern activityInternExtern,
        StripingLocksProvider<MiruStreamId> streamLocks,
        Optional<MultiChunkStore> chunkStore,
        Optional<? extends MiruResourcePartitionIdentifier> transientResource) {
        this.schema = schema;
        this.timeIndex = timeIndex;
        this.activityIndex = activityIndex;
        this.fieldIndex = fieldIndex;
        this.authzIndex = authzIndex;
        this.removalIndex = removalIndex;
        this.unreadTrackingIndex = unreadTrackingIndex;
        this.inboxIndex = inboxIndex;
        this.readTrackingWALReader = readTrackingWALReader;
        this.activityInternExtern = activityInternExtern;
        this.streamLocks = streamLocks;
        this.chunkStore = chunkStore;
        this.transientResource = transientResource;
    }

    @Override
    public MiruSchema getSchema() {
        return schema;
    }

    @Override
    public MiruTimeIndex getTimeIndex() {
        return timeIndex;
    }

    @Override
    public MiruActivityIndex getActivityIndex() {
        return activityIndex;
    }

    @Override
    public MiruFieldIndex<BM> getFieldIndex() {
        return fieldIndex;
    }

    @Override
    public MiruAuthzIndex<BM> getAuthzIndex() {
        return authzIndex;
    }

    @Override
    public MiruRemovalIndex<BM> getRemovalIndex() {
        return removalIndex;
    }

    @Override
    public MiruUnreadTrackingIndex<BM> getUnreadTrackingIndex() {
        return unreadTrackingIndex;
    }

    @Override
    public MiruInboxIndex<BM> getInboxIndex() {
        return inboxIndex;
    }

    @Override
    public MiruReadTrackingWALReader getReadTrackingWALReader() {
        return readTrackingWALReader;
    }

    @Override
    public StripingLocksProvider<MiruStreamId> getStreamLocks() {
        return streamLocks;
    }

    public long sizeInMemory() throws Exception {
        long sizeInBytes = 0;
        sizeInBytes += activityIndex.sizeInMemory();
        sizeInBytes += authzIndex.sizeInMemory();
        sizeInBytes += fieldIndex.sizeInMemory();
        sizeInBytes += inboxIndex.sizeInMemory();
        sizeInBytes += removalIndex.sizeInMemory();
        sizeInBytes += timeIndex.sizeInMemory();
        sizeInBytes += unreadTrackingIndex.sizeInMemory();
        return sizeInBytes;
    }

    public long sizeOnDisk() throws Exception {
        long sizeInBytes = 0;
        sizeInBytes += activityIndex.sizeOnDisk();
        sizeInBytes += authzIndex.sizeOnDisk();
        sizeInBytes += fieldIndex.sizeOnDisk();
        sizeInBytes += inboxIndex.sizeOnDisk();
        sizeInBytes += removalIndex.sizeOnDisk();
        sizeInBytes += timeIndex.sizeOnDisk();
        sizeInBytes += unreadTrackingIndex.sizeOnDisk();
        if (chunkStore.isPresent()) {
            sizeInBytes += chunkStore.get().sizeInBytes();
        }
        return sizeInBytes;
    }
}
