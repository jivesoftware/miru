package com.jivesoftware.os.miru.api.topology;

import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class MiruTopologyResponse {

    public List<MiruTopologyPartition> topology;

    public MiruTopologyResponse() {
    }

    public MiruTopologyResponse(List<MiruTopologyPartition> topology) {
        this.topology = topology;
    }

}
