package com.jivesoftware.os.miru.cluster.memory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.MiruHostsPartitionsStorage;
import java.util.List;
import java.util.Set;

/**
 *
 * @author jonathan
 */
public class MiruInMemoryHostsPartitionsStorage implements MiruHostsPartitionsStorage {

    private final Multimap<MiruHost, MiruPartitionCoord> hostToPartitions;

    public MiruInMemoryHostsPartitionsStorage() {
        this.hostToPartitions = HashMultimap.create();
    }

    @Override
    public void add(MiruHost host, List<MiruPartitionCoord> partitions) {
        this.hostToPartitions.putAll(host, partitions);
    }

    @Override
    public void remove(MiruHost host, List<MiruPartitionCoord> partitions) {
        for (MiruPartitionCoord partition : partitions) {
            this.hostToPartitions.remove(host, partition);
        }
    }

    @Override
    public Set<MiruPartitionCoord> getPartitions(MiruTenantId tenantId, MiruHost host) {
        Set<MiruPartitionCoord> partitions = Sets.newHashSet();
        for (MiruPartitionCoord partition : hostToPartitions.get(host)) {
            if (partition.tenantId.equals(tenantId)) {
                partitions.add(partition);
            }
        }
        return partitions;
    }
}
