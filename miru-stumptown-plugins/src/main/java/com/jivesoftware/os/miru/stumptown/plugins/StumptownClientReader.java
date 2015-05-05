package com.jivesoftware.os.miru.stumptown.plugins;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.reader.cluster.MiruClusterReader;

/**
 *
 */
public class StumptownClientReader {

    private final MiruClusterReader clusterReader;

    public StumptownClientReader(MiruClusterReader clusterReader) {
        this.clusterReader = clusterReader;
    }

    public StumptownAnswer buildWaveform(MiruTenantId tenantId,
        Optional<MiruActorId> actorId,
        StumptownQuery query) throws MiruQueryServiceException {

        try {
            return clusterReader.read(tenantId, actorId, query,
                    StumptownConstants.STUMPTOWN_PREFIX + StumptownConstants.CUSTOM_QUERY_ENDPOINT,
                    StumptownAnswer.class, StumptownAnswer.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed build waveform", e);
        }
    }

}
