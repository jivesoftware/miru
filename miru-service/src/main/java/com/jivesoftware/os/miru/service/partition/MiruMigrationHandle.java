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
public interface MiruMigrationHandle<BM, C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> extends AutoCloseable {

    boolean canMigrateTo(MiruBackingStorage destinationStorage);

    Optional<MiruContext<BM, S>> getContext();

    Optional<MiruContext<BM, S>> closeContext();

    void merge(MiruMergeChits chits, ExecutorService mergeExecutor) throws Exception;

    MiruPartitionAccessor<BM, C, S> migrated(MiruContext<BM, S> stream, Optional<MiruBackingStorage> storage, Optional<MiruPartitionState> state);
}
