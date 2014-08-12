package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.service.stream.MiruStream;

/**
 *
 */
public interface MiruMigrationHandle extends AutoCloseable {

    boolean canMigrateTo(MiruBackingStorage destinationStorage);

    MiruStream getStream();

    MiruPartitionStreamGate migrated(MiruStream stream, Optional<MiruBackingStorage> storage, Optional<MiruPartitionState> state, long sipTimestamp);
}
