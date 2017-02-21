package com.jivesoftware.os.miru.service.partition;

import com.google.common.collect.Lists;
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
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class MiruPartitionHeartbeatHandler {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruClusterClient clusterClient;
    private final AtomicBoolean atleastOneThumpThump;

    private final Map<MiruPartitionCoord, PartitionInfo> heartbeats = Maps.newConcurrentMap();
    private final Map<MiruPartitionCoord, MiruPartitionActive> active = Maps.newConcurrentMap();

    private final Object cursorLock = new Object();
    private final Map<String, NamedCursor> partitionActiveUpdatesSinceCursors = Maps.newHashMap();
    private final Map<String, NamedCursor> topologyUpdatesSinceCursors = Maps.newHashMap();
    private final AtomicBoolean destructionPermit = new AtomicBoolean();

    public MiruPartitionHeartbeatHandler(MiruClusterClient clusterClient, AtomicBoolean atleastOneThumpThump) {
        this.clusterClient = clusterClient;
        this.atleastOneThumpThump = atleastOneThumpThump;
    }

    public void updateInfo(MiruPartitionCoord coord, MiruPartitionCoordInfo info) throws Exception {
        updateHeartbeat(coord, info, -1, -1);
    }

    public void updateQueryTimestamp(MiruPartitionCoord coord, long queryTimestamp) throws Exception {
        updateHeartbeat(coord, null, queryTimestamp, -1);
    }

    public void updateLastId(MiruPartitionCoord coord, int lastId) throws Exception {
        updateHeartbeat(coord, null, -1, lastId);
    }

    private void updateHeartbeat(MiruPartitionCoord coord,
        MiruPartitionCoordInfo info,
        long queryTimestamp,
        int lastId)
        throws Exception {

        heartbeats.compute(coord, (key, got) -> {
            if (got == null) {
                return new PartitionInfo(coord.tenantId,
                    coord.partitionId.getId(),
                    queryTimestamp,
                    info,
                    lastId);
            } else {
                return new PartitionInfo(coord.tenantId,
                    coord.partitionId.getId(),
                    Math.max(queryTimestamp, got.queryTimestamp),
                    info != null ? info : got.info,
                    lastId);
            }
        });
    }

    private void retry(MiruHost host, PartitionInfo partitionInfo) throws Exception {
        MiruPartitionCoord coord = new MiruPartitionCoord(partitionInfo.tenantId, MiruPartitionId.of(partitionInfo.partitionId), host);
        heartbeats.compute(coord, (key, got) -> {
            if (got == null) {
                return partitionInfo;
            } else {
                return new PartitionInfo(coord.tenantId,
                    coord.partitionId.getId(),
                    Math.max(partitionInfo.queryTimestamp, got.queryTimestamp),
                    got.info != null ? got.info : partitionInfo.info,
                    Math.max(partitionInfo.lastId, got.lastId));
            }
        });
    }

    public MiruHeartbeatResponse thumpthump(MiruHost host) throws Exception {
        synchronized (cursorLock) {
            Collection<PartitionInfo> heartbeats = heartbeats();
            MiruHeartbeatResponse thumpthump;
            try {
                thumpthump = clusterClient.thumpthump(host,
                    new MiruHeartbeatRequest(heartbeats, partitionActiveUpdatesSinceCursors.values(), topologyUpdatesSinceCursors.values()));
            } catch (Exception e) {
                for (PartitionInfo partitionInfo : heartbeats) {
                    retry(host, partitionInfo);
                }
                throw e;
            }

            if (thumpthump != null) {
                if (thumpthump.activeHasChanged != null) {
                    setActive(host, thumpthump.activeHasChanged.result);
                    handleCursors(partitionActiveUpdatesSinceCursors, thumpthump.activeHasChanged.cursors);
                } else {
                    LOG.warn("Missing thumpthump active changes");
                }

                if (thumpthump.topologyHasChanged != null) {
                    handleCursors(topologyUpdatesSinceCursors, thumpthump.topologyHasChanged.cursors);
                } else {
                    LOG.warn("Missing thumpthump topology changes");
                }
            } else {
                LOG.warn("Missing thumpthump response");
            }
            atleastOneThumpThump.set(true);
            return thumpthump;
        }
    }

    public MiruPartitionActive getPartitionActive(MiruPartitionCoord coord) throws Exception {
        MiruPartitionActive partitionActive = active.get(coord);
        if (partitionActive != null) {
            return partitionActive;
        }
        return new MiruPartitionActive(-1, -1, -1, -1, -1);
    }

    private Collection<PartitionInfo> heartbeats() {
        Set<MiruPartitionCoord> keys = heartbeats.keySet();
        List<PartitionInfo> beats = Lists.newArrayListWithExpectedSize(keys.size());
        for (MiruPartitionCoord coord : keys) {
            PartitionInfo partitionInfo = heartbeats.remove(coord);
            if (partitionInfo != null) {
                beats.add(partitionInfo);
            }
        }
        return beats;
    }

    private void setActive(MiruHost host, Collection<MiruPartitionActiveUpdate> updates) {
        for (MiruPartitionActiveUpdate update : updates) {
            MiruPartitionCoord coord = new MiruPartitionCoord(update.tenantId, MiruPartitionId.of(update.partitionId), host);
            MiruPartitionActive partitionActive = update.partitionActive;
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

    public boolean acquireDestructionHandle() {
        return destructionPermit.compareAndSet(false, true);
    }

    public void releaseDestructionHandle() {
        destructionPermit.set(false);
    }
}
