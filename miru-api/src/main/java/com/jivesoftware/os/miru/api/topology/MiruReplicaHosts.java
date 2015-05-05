package com.jivesoftware.os.miru.api.topology;

import com.jivesoftware.os.miru.api.MiruHost;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public class MiruReplicaHosts {

    public boolean atleastOneOnline;
    public Set<MiruHost> replicaHosts;
    public int countOfMissingReplicas;

    public MiruReplicaHosts() {
    }

    public MiruReplicaHosts(boolean atleastOneOnline, Set<MiruHost> replicaHosts, int countOfMissingReplicas) {
        this.atleastOneOnline = atleastOneOnline;
        this.replicaHosts = replicaHosts;
        this.countOfMissingReplicas = countOfMissingReplicas;
    }

}
