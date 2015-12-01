package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndexProvider;
import com.jivesoftware.os.miru.plugin.index.MiruInboxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruRemovalIndex;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.index.MiruUnreadTrackingIndex;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Composes the building blocks of a MiruContext together for convenience.
 *
 * @param <IBM>
 * @author jonathan
 */
public class MiruContext<IBM, S extends MiruSipCursor<S>> implements MiruRequestContext<IBM, S> {

    public final MiruSchema schema;
    public final MiruTermComposer termComposer;
    public final MiruTimeIndex timeIndex;
    public final MiruActivityIndex activityIndex;
    public final MiruFieldIndexProvider<IBM> fieldIndexProvider;
    public final MiruSipIndex<S> sipIndex;
    public final MiruAuthzIndex<IBM> authzIndex;
    public final MiruRemovalIndex<IBM> removalIndex;
    public final MiruUnreadTrackingIndex<IBM> unreadTrackingIndex;
    public final MiruInboxIndex<IBM> inboxIndex;
    public final MiruActivityInternExtern activityInternExtern;
    public final StripingLocksProvider<MiruStreamId> streamLocks;
    public final ChunkStore[] chunkStores;
    public final Object writeLock = new Object();
    public final AtomicBoolean corrupt = new AtomicBoolean(false);
    public final AtomicInteger deltaMinId = new AtomicInteger(-1);
    public final AtomicInteger lastDeltaMinId = new AtomicInteger(-1);

    public MiruContext(MiruSchema schema,
        MiruTermComposer termComposer,
        MiruTimeIndex timeIndex,
        MiruActivityIndex activityIndex,
        MiruFieldIndexProvider<IBM> fieldIndexProvider,
        MiruSipIndex<S> sipIndex,
        MiruAuthzIndex<IBM> authzIndex,
        MiruRemovalIndex<IBM> removalIndex,
        MiruUnreadTrackingIndex<IBM> unreadTrackingIndex,
        MiruInboxIndex<IBM> inboxIndex,
        MiruActivityInternExtern activityInternExtern,
        StripingLocksProvider<MiruStreamId> streamLocks,
        ChunkStore[] chunkStores) {
        this.schema = schema;
        this.termComposer = termComposer;
        this.timeIndex = timeIndex;
        this.activityIndex = activityIndex;
        this.fieldIndexProvider = fieldIndexProvider;
        this.sipIndex = sipIndex;
        this.authzIndex = authzIndex;
        this.removalIndex = removalIndex;
        this.unreadTrackingIndex = unreadTrackingIndex;
        this.inboxIndex = inboxIndex;
        this.activityInternExtern = activityInternExtern;
        this.streamLocks = streamLocks;
        this.chunkStores = chunkStores;
    }

    @Override
    public MiruSchema getSchema() {
        return schema;
    }

    @Override
    public MiruTermComposer getTermComposer() {
        return termComposer;
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
    public MiruFieldIndexProvider<IBM> getFieldIndexProvider() {
        return fieldIndexProvider;
    }

    @Override
    public MiruSipIndex<S> getSipIndex() {
        return sipIndex;
    }

    @Override
    public MiruAuthzIndex<IBM> getAuthzIndex() {
        return authzIndex;
    }

    @Override
    public MiruRemovalIndex<IBM> getRemovalIndex() {
        return removalIndex;
    }

    @Override
    public MiruUnreadTrackingIndex<IBM> getUnreadTrackingIndex() {
        return unreadTrackingIndex;
    }

    @Override
    public MiruInboxIndex<IBM> getInboxIndex() {
        return inboxIndex;
    }

    @Override
    public StripingLocksProvider<MiruStreamId> getStreamLocks() {
        return streamLocks;
    }

    @Override
    public int getDeltaMinId() {
        return deltaMinId.get();
    }

    @Override
    public int getLastDeltaMinId() {
        return lastDeltaMinId.get();
    }

    public int markStartOfDelta(StackBuffer stackBuffer) {
        int lastId = activityIndex.lastId(stackBuffer);
        lastDeltaMinId.set(deltaMinId.getAndSet(lastId));
        return lastId;
    }

    public boolean isCorrupt() {
        return corrupt.get();
    }

    public void markCorrupt() {
        corrupt.set(true);
    }
}
