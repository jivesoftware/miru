package com.jivesoftware.os.miru.lumberyard.plugins;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.reader.cluster.MiruClusterReader;

/**
 *
 */
public class LumberyardClientReader {

    private final MiruClusterReader clusterReader;

    public LumberyardClientReader(MiruClusterReader clusterReader) {
        this.clusterReader = clusterReader;
    }

    public LumberyardAnswer buildWaveform(MiruTenantId tenantId,
        Optional<MiruActorId> actorId,
        LumberyardQuery query) throws MiruQueryServiceException {

        try {
            return clusterReader.read(tenantId, actorId, query,
                    LumberyardConstants.LUMBERYARD_PREFIX + LumberyardConstants.CUSTOM_QUERY_ENDPOINT,
                    LumberyardAnswer.class, LumberyardAnswer.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed build waveform", e);
        }
    }

}
