package com.jivesoftware.os.miru.wal.deployable.region.input;

import com.jivesoftware.os.miru.api.topology.MiruTopologyStatus;
import com.jivesoftware.os.miru.api.topology.HostHeartbeat;
import java.util.List;

/**
 *
 */
public class MiruHostEntryRegionInput {

    public final HostHeartbeat hostHeartbeat;
    public final List<MiruTopologyStatus> topologies;

    public MiruHostEntryRegionInput(HostHeartbeat hostHeartbeat, List<MiruTopologyStatus> topologies) {
        this.hostHeartbeat = hostHeartbeat;
        this.topologies = topologies;
    }
}
