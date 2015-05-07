package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.topology.MiruHeartbeatRequest;
import com.jivesoftware.os.miru.api.topology.MiruHeartbeatResponse;
import com.jivesoftware.os.miru.api.topology.MiruPartitionActive;
import com.jivesoftware.os.miru.api.topology.MiruPartitionActiveUpdate;
import com.jivesoftware.os.miru.api.topology.NamedCursor;
import com.jivesoftware.os.miru.api.topology.PartitionInfo;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class MiruPartitionHeartbeatHandler {

    private final MiruClusterClient clusterClient;

    private final AtomicReference<Map<MiruPartitionCoord, PartitionInfo>> heartbeats = new AtomicReference<>(
        (Map<MiruPartitionCoord, PartitionInfo>) new ConcurrentHashMap<MiruPartitionCoord, PartitionInfo>());
    private final ConcurrentMap<MiruPartitionCoord, MiruPartitionActive> active = Maps.newConcurrentMap();

    private final Object cursorLock = new Object();
    private final Map<String, NamedCursor> partitionActiveUpdatesSinceCursors = Maps.newHashMap();
    private final Map<String, NamedCursor> topologyUpdatesSinceCursors = Maps.newHashMap();

    public MiruPartitionHeartbeatHandler(MiruClusterClient clusterClient) {
        this.clusterClient = clusterClient;
    }

    public void heartbeat(MiruPartitionCoord coord,
        Optional<MiruPartitionCoordInfo> info,
        Optional<Long> ingressTimestamp,
        Optional<Long> queryTimestamp)
        throws Exception {

        Map<MiruPartitionCoord, PartitionInfo> beats;
        do {
            beats = heartbeats.get();
            PartitionInfo got = beats.get(coord);
            if (got == null) {
                got = new PartitionInfo(coord.tenantId,
                    coord.partitionId.getId(),
                    ingressTimestamp.or(-1L),
                    queryTimestamp.or(-1L),
                    info.orNull());
                beats.put(coord, got);
            } else {
                got = new PartitionInfo(coord.tenantId,
                    coord.partitionId.getId(),
                    ingressTimestamp.or(got.ingressTimestamp),
                    queryTimestamp.or(got.queryTimestamp),
                    info.isPresent() ? info.get() : got.info);
                beats.put(coord, got);
            }
        }
        while (beats != heartbeats.get());
    }

    public MiruHeartbeatResponse thumpthump(MiruHost host) throws Exception {
        synchronized (cursorLock) {
            MiruHeartbeatResponse thumpthump = clusterClient.thumpthump(host,
                new MiruHeartbeatRequest(heartbeats(), partitionActiveUpdatesSinceCursors.values(), topologyUpdatesSinceCursors.values()));

            setActive(host, thumpthump.activeHasChanged.result);
            handleCursors(partitionActiveUpdatesSinceCursors, thumpthump.activeHasChanged.cursors);
            handleCursors(topologyUpdatesSinceCursors, thumpthump.topologyHasChanged.cursors);

            return thumpthump;
        }
    }

    public MiruPartitionActive getPartitionActive(MiruPartitionCoord coord) throws Exception {
        MiruPartitionActive partitionActive = active.get(coord);
        if (partitionActive != null) {
            return partitionActive;
        }
        return new MiruPartitionActive(-1, -1, -1);
    }

    private Collection<PartitionInfo> heartbeats() {
        Map<MiruPartitionCoord, PartitionInfo> beats = heartbeats.get();
        if (beats != null) {
            heartbeats.compareAndSet(beats, new ConcurrentHashMap<MiruPartitionCoord, PartitionInfo>());
            return beats.values();
        } else {
            return Collections.emptyList();
        }
    }

    private void setActive(MiruHost host, Collection<MiruPartitionActiveUpdate> updates) {
        for (MiruPartitionActiveUpdate update : updates) {
            MiruPartitionCoord coord = new MiruPartitionCoord(update.tenantId, MiruPartitionId.of(update.partitionId), host);
            MiruPartitionActive partitionActive = new MiruPartitionActive(update.activeUntilTimestamp,
                update.idleAfterTimestamp, update.destroyAfterTimestamp);
            active.put(coord, partitionActive);
        }
    }

    /**
     * Must be holding cursor lock.
     */
    private void handleCursors(Map<String, NamedCursor> cursors, Collection<NamedCursor> updates) {
        for (NamedCursor update : updates) {
            NamedCursor existing = cursors.get(update.name);
            if (existing == null || existing.id < update.id) {
                cursors.put(update.name, update);
            }
        }
    }

}
