package com.jivesoftware.os.miru.cluster.rcvs;

import com.google.common.collect.ComparisonChain;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;

/**
*
*/
public class MiruTopologyColumnKey implements Comparable<MiruTopologyColumnKey> {

    public final MiruPartitionId partitionId;
    public final MiruHost host;

    public MiruTopologyColumnKey(MiruPartitionId partitionId, MiruHost host) {
        this.partitionId = partitionId;
        this.host = host;
    }

    @Override
    public int compareTo(MiruTopologyColumnKey miruTopologyColumnKey) {
        return ComparisonChain.start()
            .compare(partitionId.getId(), miruTopologyColumnKey.partitionId.getId())
            .compare(host, miruTopologyColumnKey.host)
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

        MiruTopologyColumnKey that = (MiruTopologyColumnKey) o;

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
