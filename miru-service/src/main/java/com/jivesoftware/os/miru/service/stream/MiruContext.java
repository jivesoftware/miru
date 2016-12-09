package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider;
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
import com.jivesoftware.os.miru.service.index.Closeable;
import com.jivesoftware.os.miru.service.index.Commitable;
import com.jivesoftware.os.miru.service.index.Compactable;
import com.jivesoftware.os.miru.service.index.Removable;
import com.jivesoftware.os.miru.service.index.TimeIdIndex;
import com.jivesoftware.os.miru.service.stream.MiruRebuildDirector.Token;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Composes the building blocks of a MiruContext together for convenience.
 *
 * @param <IBM>
 * @author jonathan
 */
public class MiruContext<BM extends IBM, IBM, S extends MiruSipCursor<S>> implements MiruRequestContext<BM, IBM, S> {

    public final long version;
    public final TimeIdIndex timeIdIndex;
    public final MiruSchema schema;
    public final MiruTermComposer termComposer;
    public final MiruTimeIndex timeIndex;
    public final MiruActivityIndex activityIndex;
    public final MiruFieldIndexProvider<BM, IBM> fieldIndexProvider;
    public final MiruSipIndex<S> sipIndex;
    public final MiruAuthzIndex<BM, IBM> authzIndex;
    public final MiruRemovalIndex<BM, IBM> removalIndex;
    public final MiruUnreadTrackingIndex<BM, IBM> unreadTrackingIndex;
    public final MiruInboxIndex<BM, IBM> inboxIndex;
    public final MiruPluginCacheProvider cacheProvider;
    public final MiruActivityInternExtern activityInternExtern;
    public final StripingLocksProvider<MiruStreamId> streamLocks;
    public final ChunkStore[] chunkStores;
    public final LABEnvironment[] labEnvironments;
    public final MiruBackingStorage storage;
    public final Object writeLock = new Object();
    public final AtomicBoolean corrupt = new AtomicBoolean(false);
    public final MiruRebuildDirector.Token rebuildToken;
    public final AtomicBoolean closed = new AtomicBoolean(false);
    public final Commitable commitable;
    public final Closeable closeable;
    public final Removable removable;
    public final Compactable compactable;

    public MiruContext(long version,
        TimeIdIndex timeIdIndex,
        MiruSchema schema,
        MiruTermComposer termComposer,
        MiruTimeIndex timeIndex,
        MiruActivityIndex activityIndex,
        MiruFieldIndexProvider<BM, IBM> fieldIndexProvider,
        MiruSipIndex<S> sipIndex,
        MiruAuthzIndex<BM, IBM> authzIndex,
        MiruRemovalIndex<BM, IBM> removalIndex,
        MiruUnreadTrackingIndex<BM, IBM> unreadTrackingIndex,
        MiruInboxIndex<BM, IBM> inboxIndex,
        MiruPluginCacheProvider cacheProvider,
        MiruActivityInternExtern activityInternExtern,
        StripingLocksProvider<MiruStreamId> streamLocks,
        ChunkStore[] chunkStores,
        LABEnvironment[] labEnvironments,
        MiruBackingStorage storage,
        Token rebuildToken,
        Commitable commitable,
        Closeable closeable,
        Removable removable, Compactable compactable) {
        this.version = version;
        this.timeIdIndex = timeIdIndex;
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
        this.cacheProvider = cacheProvider;
        this.activityInternExtern = activityInternExtern;
        this.streamLocks = streamLocks;
        this.chunkStores = chunkStores;
        this.labEnvironments = labEnvironments;
        this.storage = storage;
        this.rebuildToken = rebuildToken;
        this.commitable = commitable;
        this.closeable = closeable;
        this.removable = removable;
        this.compactable = compactable;
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
    public MiruFieldIndexProvider<BM, IBM> getFieldIndexProvider() {
        return fieldIndexProvider;
    }

    @Override
    public MiruSipIndex<S> getSipIndex() {
        return sipIndex;
    }

    @Override
    public MiruAuthzIndex<BM, IBM> getAuthzIndex() {
        return authzIndex;
    }

    @Override
    public MiruRemovalIndex<BM, IBM> getRemovalIndex() {
        return removalIndex;
    }

    @Override
    public MiruUnreadTrackingIndex<BM, IBM> getUnreadTrackingIndex() {
        return unreadTrackingIndex;
    }

    @Override
    public MiruInboxIndex<BM, IBM> getInboxIndex() {
        return inboxIndex;
    }

    @Override
    public MiruPluginCacheProvider getCacheProvider() {
        return cacheProvider;
    }

    @Override
    public StripingLocksProvider<MiruStreamId> getStreamLocks() {
        return streamLocks;
    }

    @Override
    public boolean hasChunkStores() {
        return chunkStores != null;
    }

    @Override
    public boolean hasLabIndex() {
        return labEnvironments != null;
    }

    public boolean isCorrupt() {
        return corrupt.get();
    }

    public void markCorrupt() {
        corrupt.set(true);
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    public void markClosed() {
        closed.set(true);
    }
}
