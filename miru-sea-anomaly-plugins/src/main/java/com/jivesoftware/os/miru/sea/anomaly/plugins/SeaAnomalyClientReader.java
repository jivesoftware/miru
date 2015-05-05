package com.jivesoftware.os.miru.sea.anomaly.plugins;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.reader.cluster.MiruClusterReader;

/**
 *
 */
public class SeaAnomalyClientReader {

    private final MiruClusterReader clusterReader;

    public SeaAnomalyClientReader(MiruClusterReader clusterReader) {
        this.clusterReader = clusterReader;
    }

    public SeaAnomalyAnswer buildWaveform(MiruTenantId tenantId,
        Optional<MiruActorId> actorId,
        SeaAnomalyQuery query) throws MiruQueryServiceException {

        try {
            return clusterReader.read(tenantId, actorId, query,
                    SeaAnomalyConstants.SEA_ANOMALY_PREFIX + SeaAnomalyConstants.CUSTOM_QUERY_ENDPOINT,
                    SeaAnomalyAnswer.class, SeaAnomalyAnswer.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed build waveform", e);
        }
    }

}
