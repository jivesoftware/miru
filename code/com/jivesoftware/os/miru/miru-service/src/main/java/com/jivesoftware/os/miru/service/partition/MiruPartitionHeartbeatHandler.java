package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.miru.api.topology.MiruPartitionActive;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.topology.MiruHeartbeatRequest;
import com.jivesoftware.os.miru.api.topology.MiruHeartbeatResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class MiruPartitionHeartbeatHandler {

    private final AtomicReference<Map<MiruPartitionCoord, MiruHeartbeatRequest.Partition>> heartbeats = new AtomicReference<>();
    private final AtomicReference<Map<MiruPartitionCoord, MiruPartitionActive>> active = new AtomicReference<>();
    private final MiruClusterClient clusterClient;

    public MiruPartitionHeartbeatHandler(MiruClusterClient clusterClient) {
        this.clusterClient = clusterClient;
    }

    public void heartbeat(MiruPartitionCoord coord,
        MiruPartitionCoordInfo info,
        Optional<Long> refreshTimestamp)
        throws Exception {

        Map<MiruPartitionCoord, MiruHeartbeatRequest.Partition> beats;
        do {
            beats = heartbeats.get();
            MiruHeartbeatRequest.Partition got = beats.get(coord);
            if (got == null) {
                got = new MiruHeartbeatRequest.Partition(coord.tenantId.getBytes(),
                    coord.partitionId.getId(),
                    refreshTimestamp.or(-1L),
                    info.state,
                    info.storage
                );
                beats.put(coord, got);
            } else {
                got = new MiruHeartbeatRequest.Partition(coord.tenantId.getBytes(),
                    coord.partitionId.getId(),
                    refreshTimestamp.or(got.activeTimestamp),
                    info.state,
                    info.storage
                );
                beats.put(coord, got);
            }
        } while (beats != heartbeats.get());
    }

    public MiruHeartbeatResponse thumpthump(MiruHost host) throws Exception {
        MiruHeartbeatResponse thumpthump = clusterClient.thumpthump(host, new MiruHeartbeatRequest(heartbeats()));
        setActive(host, thumpthump.active);
        return thumpthump;
    }

    public MiruPartitionActive isCoordActive(MiruPartitionCoord coord) throws Exception {
        Map<MiruPartitionCoord, MiruPartitionActive> got = active.get();
        if (got != null) {
            MiruPartitionActive partitionActive = got.get(coord);
            if (partitionActive != null) {
                return partitionActive;
            }
        }
        return new MiruPartitionActive(false, false);
    }

    private List<MiruHeartbeatRequest.Partition> heartbeats() {
        Map<MiruPartitionCoord, MiruHeartbeatRequest.Partition> beats = heartbeats.get();
        heartbeats.compareAndSet(beats, new ConcurrentHashMap<MiruPartitionCoord, MiruHeartbeatRequest.Partition>());
        return new ArrayList<>(beats.values());
    }

    private void setActive(MiruHost host, List<MiruHeartbeatResponse.Partition> coords) {
        Map<MiruPartitionCoord, MiruPartitionActive> coordsActives = new HashMap<>(coords.size());
        for (MiruHeartbeatResponse.Partition coord : coords) {
            MiruPartitionCoord miruPartitionCoord = new MiruPartitionCoord(new MiruTenantId(coord.tenantId), MiruPartitionId.of(coord.partitionId), host);
            MiruPartitionActive partitionActive = new MiruPartitionActive(coord.active, coord.idle);
            coordsActives.put(miruPartitionCoord, partitionActive);
        }
        active.set(coordsActives);
    }

}
