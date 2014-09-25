package com.jivesoftware.os.miru.service.partition;

import com.google.common.collect.ComparisonChain;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;

/**
 * Package private encapsulation of partitionId and host.
 */
class PartitionAndHost implements Comparable<PartitionAndHost> {

    final MiruPartitionId partitionId;
    final MiruHost host;

    PartitionAndHost(MiruPartitionId partitionId, MiruHost host) {
        this.partitionId = partitionId;
        this.host = host;
    }

    @Override
    public int compareTo(PartitionAndHost o) {
        return ComparisonChain
            .start()
            .compare(partitionId.getId(), o.partitionId.getId())
            .compare(host.getPort(), o.host.getPort())
            .compare(host.getLogicalName(), o.host.getLogicalName())
            .result();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PartitionAndHost that = (PartitionAndHost) o;

        if (host != null ? !host.equals(that.host) : that.host != null) {
            return false;
        }
        if (partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = partitionId != null ? partitionId.hashCode() : 0;
        result = 31 * result + (host != null ? host.hashCode() : 0);
        return result;
    }
}
