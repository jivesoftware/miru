package com.jivesoftware.os.miru.service.partition;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class PartitionErrorTracker {

    private final Set<MiruPartitionCoord> errorsSinceStartup = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Map<MiruPartitionCoord, List<String>> errorsSinceRebuild = new ConcurrentHashMap<>();

    public void error(MiruPartitionCoord coord, String reason) {
        errorsSinceStartup.add(coord);
        errorsSinceRebuild.compute(coord, (key, reasons) -> {
            if (reasons == null) {
                reasons = Lists.newArrayList();
            }
            reasons.add(reason);
            return reasons;
        });
    }

    public void reset(MiruPartitionCoord coord) {
        errorsSinceRebuild.remove(coord);
    }

    public Set<MiruPartitionCoord> getErrorsSinceStartup() {
        return Collections.unmodifiableSet(errorsSinceStartup);
    }

    public Map<MiruPartitionCoord, List<String>> getErrorsSinceRebuild() {
        return Collections.unmodifiableMap(errorsSinceRebuild);
    }

    public TrackError track(MiruPartitionCoord coord) {
        return new TrackCoord(coord);
    }

    private class TrackCoord implements TrackError {

        private final MiruPartitionCoord coord;

        public TrackCoord(MiruPartitionCoord coord) {
            this.coord = coord;
        }

        @Override
        public void error(String reason) {
            PartitionErrorTracker.this.error(coord, reason);
        }

        @Override
        public void reset() {
            PartitionErrorTracker.this.reset(coord);
        }
    }
}
