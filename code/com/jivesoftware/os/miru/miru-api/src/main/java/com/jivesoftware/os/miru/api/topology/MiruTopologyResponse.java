package com.jivesoftware.os.miru.api.topology;

import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class MiruTopologyResponse {

    public List<Partition> topology;

    public MiruTopologyResponse() {
    }

    public MiruTopologyResponse(List<Partition> topology) {
        this.topology = topology;
    }

    static public class Partition {

        public MiruHost host;
        public int partitionId;
        public MiruPartitionState state;
        public MiruBackingStorage storage;

        public Partition() {
        }

        public Partition(MiruHost miruHost, int partitionId, MiruPartitionState state, MiruBackingStorage storage) {
            this.host = miruHost;
            this.partitionId = partitionId;
            this.state = state;
            this.storage = storage;
        }

    }
}
