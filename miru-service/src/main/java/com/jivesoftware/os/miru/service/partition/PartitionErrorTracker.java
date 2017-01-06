package com.jivesoftware.os.miru.service.partition;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.health.HealthCheck;
import com.jivesoftware.os.routing.bird.health.HealthCheckResponse;
import com.jivesoftware.os.routing.bird.health.HealthCheckResponseImpl;
import com.jivesoftware.os.routing.bird.health.api.HealthCheckConfig;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.merlin.config.defaults.FloatDefault;
import org.merlin.config.defaults.StringDefault;

/**
 *
 */
public class PartitionErrorTracker implements HealthCheck {

    public interface PartitionErrorTrackerConfig extends HealthCheckConfig {

        @StringDefault("partitionErrorTracker")
        @Override
        String getName();

        @StringDefault("Whether partitions are in error and should be rebuilt from the UI")
        @Override
        String getDescription();

        @FloatDefault(0.4f)
        Float getHealthOnError();
    }

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final PartitionErrorTrackerConfig config;

    private final Set<MiruPartitionCoord> errorsBeforeRebuild = Collections.newSetFromMap(Maps.newConcurrentMap());
    private final Map<MiruPartitionCoord, Set<String>> errorsSinceRebuild = Maps.newConcurrentMap();

    public PartitionErrorTracker(PartitionErrorTrackerConfig config) {
        this.config = config;
    }

    @Override
    public HealthCheckResponse checkHealth() throws Exception {
        double health = errorsSinceRebuild.isEmpty() ? 1.0 : config.getHealthOnError();
        String status = "Partitions in error: " + errorsSinceRebuild.size();
        String resolution = "Rebuild partitions in error from the UI";
        return new HealthCheckResponseImpl(config.getName(), health, status, config.getDescription(), resolution, System.currentTimeMillis());
    }

    public void error(MiruPartitionCoord coord, String reason) {
        LOG.info("Partition error tracker recorded coord:{} reason:{}", coord, reason);
        errorsSinceRebuild.compute(coord, (key, reasons) -> {
            if (reasons == null) {
                reasons = Sets.newHashSet();
            }
            reasons.add(reason);
            return reasons;
        });
    }

    public void reset(MiruPartitionCoord coord) {
        errorsSinceRebuild.remove(coord);
        errorsBeforeRebuild.add(coord);
    }

    public Set<MiruPartitionCoord> getErrorsBeforeRebuild() {
        return Collections.unmodifiableSet(errorsBeforeRebuild);
    }

    public Map<MiruPartitionCoord, Set<String>> getErrorsSinceRebuild() {
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
