package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.reader.cluster.MiruClusterReader;

/**
 *
 */
public class DistinctsClientReader {

    private final MiruClusterReader clusterReader;

    public DistinctsClientReader(MiruClusterReader clusterReader) {
        this.clusterReader = clusterReader;
    }

    public DistinctsAnswer gatherDistincts(MiruTenantId tenantId,
            Optional<MiruActorId> actorId,
            DistinctsQuery query)
            throws MiruQueryServiceException {

        try {
            return clusterReader.read(tenantId, actorId, query,
                    DistinctsConstants.DISTINCTS_PREFIX + DistinctsConstants.CUSTOM_QUERY_ENDPOINT,
                    DistinctsAnswer.class, DistinctsAnswer.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed gather distincts", e);
        }
    }

}
