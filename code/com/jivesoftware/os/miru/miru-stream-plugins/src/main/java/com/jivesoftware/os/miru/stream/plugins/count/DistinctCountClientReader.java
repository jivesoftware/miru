package com.jivesoftware.os.miru.stream.plugins.count;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.reader.cluster.MiruClusterReader;

import static com.jivesoftware.os.miru.stream.plugins.count.DistinctCountConstants.COUNT_PREFIX;
import static com.jivesoftware.os.miru.stream.plugins.count.DistinctCountConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.stream.plugins.count.DistinctCountConstants.INBOX_ALL_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.stream.plugins.count.DistinctCountConstants.INBOX_UNREAD_QUERY_ENDPOINT;

/**
 *
 */
public class DistinctCountClientReader {

    private final MiruClusterReader clusterReader;

    public DistinctCountClientReader(MiruClusterReader clusterReader) {
        this.clusterReader = clusterReader;
    }

    public DistinctCountResult countCustomStream(MiruTenantId tenantId,
            Optional<MiruActorId> actorId,
            DistinctCountQuery query)
            throws MiruQueryServiceException {

        try {
            return clusterReader.read(tenantId, actorId, query,
                    COUNT_PREFIX + CUSTOM_QUERY_ENDPOINT,
                    DistinctCountResult.class, DistinctCountResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed count custom stream", e);
        }
    }

    public DistinctCountResult countInboxStreamAll(MiruTenantId tenantId,
            Optional<MiruActorId> actorId,
            DistinctCountQuery query)
            throws MiruQueryServiceException {

        try {
            return clusterReader.read(tenantId, actorId, query,
                    COUNT_PREFIX + INBOX_ALL_QUERY_ENDPOINT,
                    DistinctCountResult.class, DistinctCountResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed count inbox all stream", e);
        }
    }

    public DistinctCountResult countInboxStreamUnread(MiruTenantId tenantId,
            Optional<MiruActorId> actorId,
            DistinctCountQuery query)
            throws MiruQueryServiceException {

        try {
            return clusterReader.read(tenantId, actorId, query,
                    COUNT_PREFIX + INBOX_UNREAD_QUERY_ENDPOINT,
                    DistinctCountResult.class, DistinctCountResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed count inbox unread stream", e);
        }
    }

}
