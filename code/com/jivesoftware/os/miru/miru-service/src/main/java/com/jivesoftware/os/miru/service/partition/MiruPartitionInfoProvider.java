package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import java.util.Iterator;

/**
 *
 */
public interface MiruPartitionInfoProvider {

    Optional<MiruPartitionCoordInfo> get(MiruPartitionCoord coord);

    void put(MiruPartitionCoord partitionCoord, MiruPartitionCoordInfo partitionCoordInfo);

    Iterator<MiruPartitionCoord> getKeysIterator();
}
