package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import com.jivesoftware.os.miru.service.stream.MiruContextFactory;
import java.util.concurrent.ExecutorService;

/**
 *
 */
public interface MiruMigrationHandle<BM extends IBM, IBM, C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> extends AutoCloseable {

    boolean canMigrateTo(MiruBackingStorage destinationStorage);

    Optional<MiruContext<IBM, S>> getContext();

    void closePersistentContext(MiruContextFactory<S> contextFactory);

    void closeTransientContext(MiruContextFactory<S> contextFactory);

    void merge(ExecutorService mergeExecutor, Optional<MiruContext<IBM, S>> context, MiruMergeChits chits, TrackError trackError) throws Exception;

    MiruPartitionAccessor<BM, IBM, C, S> migrated(Optional<MiruContext<IBM, S>> newPersistentContext,
                Optional<MiruContext<IBM, S>> newTransientContext,
                Optional<MiruPartitionState> newState,
                Optional<Boolean> newHasPersistentStorage);
}
