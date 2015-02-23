package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
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
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;

/**
 * Composes the building blocks of a MiruContext together for convenience.
 *
 * @param <BM>
 * @author jonathan
 */
public class MiruContext<BM> implements MiruRequestContext<BM> {

    public final MiruSchema schema;
    public final MiruTermComposer termComposer;
    public final MiruTimeIndex timeIndex;
    public final MiruActivityIndex activityIndex;
    public final MiruFieldIndexProvider<BM> fieldIndexProvider;
    public final MiruSipIndex sipIndex;
    public final MiruAuthzIndex<BM> authzIndex;
    public final MiruRemovalIndex<BM> removalIndex;
    public final MiruUnreadTrackingIndex<BM> unreadTrackingIndex;
    public final MiruInboxIndex<BM> inboxIndex;
    public final MiruReadTrackingWALReader readTrackingWALReader;
    public final MiruActivityInternExtern activityInternExtern;
    public final StripingLocksProvider<MiruStreamId> streamLocks;
    public final ChunkStore[] chunkStores;
    public final Object writeLock = new Object();

    public MiruContext(MiruSchema schema,
        MiruTermComposer termComposer,
        MiruTimeIndex timeIndex,
        MiruActivityIndex activityIndex,
        MiruFieldIndexProvider<BM> fieldIndexProvider,
        MiruSipIndex sipIndex, MiruAuthzIndex<BM> authzIndex,
        MiruRemovalIndex<BM> removalIndex,
        MiruUnreadTrackingIndex<BM> unreadTrackingIndex,
        MiruInboxIndex<BM> inboxIndex,
        MiruReadTrackingWALReader readTrackingWALReader,
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
        this.readTrackingWALReader = readTrackingWALReader;
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
    public MiruFieldIndexProvider<BM> getFieldIndexProvider() {
        return fieldIndexProvider;
    }

    @Override
    public MiruSipIndex getSipIndex() {
        return sipIndex;
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

}
