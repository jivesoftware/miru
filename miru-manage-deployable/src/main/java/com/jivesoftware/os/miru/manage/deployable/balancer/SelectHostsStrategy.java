package com.jivesoftware.os.miru.manage.deployable.balancer;

import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import java.util.List;

/**
*
*/
public interface SelectHostsStrategy {

    List<MiruHost> selectHosts(MiruHost fromHost, List<MiruHost> allHosts, List<MiruPartition> partitions, int numberOfReplicas);

    boolean isCurrentPartitionOnly();
}
