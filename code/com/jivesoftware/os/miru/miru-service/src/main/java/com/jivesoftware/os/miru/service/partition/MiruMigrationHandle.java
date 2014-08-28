package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.service.stream.MiruStream;

/**
 *
 */
public interface MiruMigrationHandle<BM> extends AutoCloseable {

    boolean canMigrateTo(MiruBackingStorage destinationStorage);

    MiruStream<BM> getStream();

    MiruPartitionStreamGate<BM> migrated(MiruStream<BM> stream, Optional<MiruBackingStorage> storage, Optional<MiruPartitionState> state, long sipTimestamp);
}
