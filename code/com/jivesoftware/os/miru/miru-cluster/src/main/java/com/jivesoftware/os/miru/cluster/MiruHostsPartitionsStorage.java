package com.jivesoftware.os.miru.cluster;

import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.List;
import java.util.Set;

public interface MiruHostsPartitionsStorage {

    void add(MiruHost host, List<MiruPartitionCoord> partitions);

    void remove(MiruHost host, List<MiruPartitionCoord> partitions);

    Set<MiruPartitionCoord> getPartitions(MiruTenantId tenantId, MiruHost host);

}
