package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import java.util.concurrent.ExecutorService;

/**
 *
 */
public interface MiruMigrationHandle<BM extends IBM, IBM, C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> extends AutoCloseable {

    boolean canMigrateTo(MiruBackingStorage destinationStorage);

    Optional<MiruContext<IBM, S>> getContext();

    Optional<MiruContext<IBM, S>> closeContext();

    void merge(MiruMergeChits chits, ExecutorService mergeExecutor, TrackError trackError) throws Exception;

    MiruPartitionAccessor<BM, IBM, C, S> migrated(MiruContext<IBM, S> stream, Optional<MiruBackingStorage> storage, Optional<MiruPartitionState> state);
}
