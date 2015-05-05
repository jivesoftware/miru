package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.reader.cluster.MiruClusterReader;

/**
 *
 */
public class AnalyticsClientReader {

    private final MiruClusterReader clusterReader;

    public AnalyticsClientReader(MiruClusterReader clusterReader) {
        this.clusterReader = clusterReader;
    }

    public AnalyticsAnswer buildWaveform(MiruTenantId tenantId,
        Optional<MiruActorId> actorId,
        AnalyticsQuery query) throws MiruQueryServiceException {

        try {
            return clusterReader.read(tenantId, actorId, query,
                    AnalyticsConstants.ANALYTICS_PREFIX + AnalyticsConstants.CUSTOM_QUERY_ENDPOINT,
                    AnalyticsAnswer.class, AnalyticsAnswer.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed build analytics waveform", e);
        }
    }

}
