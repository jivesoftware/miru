package com.jivesoftware.os.miru.stream.plugins.filter;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.reader.cluster.MiruClusterReader;

import static com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsConstants.FILTER_PREFIX;
import static com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsConstants.INBOX_ALL_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsConstants.INBOX_UNREAD_QUERY_ENDPOINT;

/**
 *
 */
public class AggregateCountsClientReader {

    private final MiruClusterReader clusterReader;

    public AggregateCountsClientReader(MiruClusterReader clusterReader) {
        this.clusterReader = clusterReader;
    }

    public AggregateCountsResult filterCustomStream(MiruTenantId tenantId,
            Optional<MiruActorId> actorId,
            AggregateCountsQuery query)
            throws MiruQueryServiceException {

        try {
            return clusterReader.read(tenantId, actorId, query,
                    FILTER_PREFIX + CUSTOM_QUERY_ENDPOINT,
                    AggregateCountsResult.class, AggregateCountsResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed filter custom stream", e);
        }
    }

    public AggregateCountsResult filterInboxStreamAll(MiruTenantId tenantId,
            Optional<MiruActorId> actorId,
            AggregateCountsQuery query)
            throws MiruQueryServiceException {

        try {
            return clusterReader.read(tenantId, actorId, query,
                    FILTER_PREFIX + INBOX_ALL_QUERY_ENDPOINT,
                    AggregateCountsResult.class, AggregateCountsResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed filter inbox all stream", e);
        }
    }

    public AggregateCountsResult filterInboxStreamUnread(MiruTenantId tenantId,
            Optional<MiruActorId> actorId,
            AggregateCountsQuery query)
            throws MiruQueryServiceException {

        try {
            return clusterReader.read(tenantId, actorId, query,
                    FILTER_PREFIX + INBOX_UNREAD_QUERY_ENDPOINT,
                    AggregateCountsResult.class, AggregateCountsResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed filter inbox unread stream", e);
        }
    }

}
