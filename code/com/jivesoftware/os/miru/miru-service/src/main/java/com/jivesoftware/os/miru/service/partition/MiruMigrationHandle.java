package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.service.stream.MiruContext;

/**
 *
 */
public interface MiruMigrationHandle<BM> extends AutoCloseable {

    boolean canMigrateTo(MiruBackingStorage destinationStorage);

    Optional<MiruContext<BM>> getContext();

    void merge() throws Exception;

    MiruPartitionAccessor<BM> migrated(MiruContext<BM> stream, Optional<MiruBackingStorage> storage, Optional<MiruPartitionState> state);
}
