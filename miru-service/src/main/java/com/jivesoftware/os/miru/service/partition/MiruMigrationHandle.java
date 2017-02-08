package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import com.jivesoftware.os.miru.service.stream.MiruContextFactory;
import com.jivesoftware.os.miru.service.stream.MiruRebuildDirector;
import java.util.concurrent.ExecutorService;

/**
 *
 */
public interface MiruMigrationHandle<BM extends IBM, IBM, C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> extends AutoCloseable {

    boolean canMigrateTo(MiruBackingStorage destinationStorage);

    Optional<MiruContext<BM, IBM, S>> getContext();

    void closePersistent(MiruContextFactory<S> contextFactory) throws Exception;

    void closeTransient(MiruContextFactory<S> contextFactory) throws Exception;

    void refundChits(MiruMergeChits mergeChits);

    void releaseRebuildTokens(MiruRebuildDirector rebuildDirector) throws Exception;

    void merge(String name, ExecutorService mergeExecutor, MiruContext<BM, IBM, S> context, MiruMergeChits chits, TrackError trackError) throws Exception;

    MiruPartitionAccessor<BM, IBM, C, S> migrated(Optional<MiruContext<BM, IBM, S>> newPersistentContext,
        Optional<MiruContext<BM, IBM, S>> newTransientContext,
        Optional<MiruPartitionState> newState,
        Optional<Boolean> newHasPersistentStorage);
}
