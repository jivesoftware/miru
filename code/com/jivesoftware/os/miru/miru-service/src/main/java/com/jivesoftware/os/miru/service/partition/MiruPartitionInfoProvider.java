package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;

/**
 *
 */
public interface MiruPartitionInfoProvider {

    Optional<MiruPartitionCoordInfo> get(MiruPartitionCoord coord);
}
