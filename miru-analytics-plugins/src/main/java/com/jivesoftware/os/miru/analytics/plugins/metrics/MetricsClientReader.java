package com.jivesoftware.os.miru.analytics.plugins.metrics;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.reader.cluster.MiruClusterReader;

/**
 *
 */
public class MetricsClientReader {

    private final MiruClusterReader clusterReader;

    public MetricsClientReader(MiruClusterReader clusterReader) {
        this.clusterReader = clusterReader;
    }

    public MetricsAnswer buildWaveform(MiruTenantId tenantId,
        Optional<MiruActorId> actorId,
        MetricsQuery query) throws MiruQueryServiceException {

        try {
            return clusterReader.read(tenantId, actorId, query,
                    MetricsConstants.METRICS_PREFIX + MetricsConstants.CUSTOM_QUERY_ENDPOINT,
                    MetricsAnswer.class, MetricsAnswer.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed build metrics waveform", e);
        }
    }

}
