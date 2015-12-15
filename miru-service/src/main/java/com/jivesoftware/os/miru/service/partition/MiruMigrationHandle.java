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

    void closePersistentContext(MiruContextFactory<S> contextFactory, MiruRebuildDirector rebuildDirector);

    void closeTransientContext(MiruContextFactory<S> contextFactory, MiruRebuildDirector rebuildDirector);

    void merge(ExecutorService mergeExecutor, Optional<MiruContext<BM, IBM, S>> context, MiruMergeChits chits, TrackError trackError) throws Exception;

    MiruPartitionAccessor<BM, IBM, C, S> migrated(Optional<MiruContext<BM, IBM, S>> newPersistentContext,
                Optional<MiruContext<BM, IBM, S>> newTransientContext,
                Optional<MiruPartitionState> newState,
                Optional<Boolean> newHasPersistentStorage);
}
