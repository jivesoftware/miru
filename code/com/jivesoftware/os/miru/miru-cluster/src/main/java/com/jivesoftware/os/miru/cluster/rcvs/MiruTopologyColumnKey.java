package com.jivesoftware.os.miru.cluster.rcvs;

import com.google.common.collect.ComparisonChain;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import java.util.Comparator;

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
            .compare(host, miruTopologyColumnKey.host, new Comparator<MiruHost>() {
                @Override
                public int compare(MiruHost o1, MiruHost o2) {
                    // favor searching local parititions first.
                    boolean localO1 = o1.equals(host);
                    boolean localO2 = o2.equals(host);
                    if (localO1 && !localO2) {
                        return -1;
                    } else if (!localO1 && localO2) {
                        return 1;
                    } else {
                        return o1.compareTo(o2);
                    }
                }
            })
            .result();
    }
}
