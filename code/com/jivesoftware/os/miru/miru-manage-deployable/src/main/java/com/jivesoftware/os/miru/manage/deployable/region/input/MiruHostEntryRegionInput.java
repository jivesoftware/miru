package com.jivesoftware.os.miru.manage.deployable.region.input;

import com.jivesoftware.os.miru.api.MiruTopologyStatus;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import java.util.List;

/**
 *
 */
public class MiruHostEntryRegionInput {

    public final MiruClusterRegistry.HostHeartbeat hostHeartbeat;
    public final List<MiruTopologyStatus> topologies;

    public MiruHostEntryRegionInput(MiruClusterRegistry.HostHeartbeat hostHeartbeat, List<MiruTopologyStatus> topologies) {
        this.hostHeartbeat = hostHeartbeat;
        this.topologies = topologies;
    }
}
