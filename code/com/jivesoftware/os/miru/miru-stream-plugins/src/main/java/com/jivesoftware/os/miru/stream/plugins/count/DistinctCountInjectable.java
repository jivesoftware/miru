package com.jivesoftware.os.miru.stream.plugins.count;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.query.Miru;
import com.jivesoftware.os.miru.query.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.query.MiruProvider;
import com.jivesoftware.os.miru.query.MiruSolvableFactory;

/**
 *
 */
public class DistinctCountInjectable {

    private final MiruProvider<? extends Miru> miruProvider;
    private final NumberOfDistincts numberOfDistincts;

    public DistinctCountInjectable(MiruProvider<? extends Miru> miruProvider, NumberOfDistincts numberOfDistincts) {
        this.miruProvider = miruProvider;
        this.numberOfDistincts = numberOfDistincts;
    }

    public DistinctCountResult countCustomStream(DistinctCountQuery query) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.callAndMerge(tenantId,
                    new MiruSolvableFactory<>(new CountCustomExecuteQuery(numberOfDistincts, query)),
                    new DistinctCountResultEvaluator(query),
                    new MergeDistinctCountResults(),
                    DistinctCountResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count custom stream", e);
        }
    }

    public DistinctCountResult countInboxStreamAll(DistinctCountQuery query) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.callAndMerge(tenantId,
                    new MiruSolvableFactory<>(new CountInboxExecuteQuery(numberOfDistincts,
                            miruProvider.getBackfillerizer(tenantId), query, false)),
                    new DistinctCountResultEvaluator(query),
                    new MergeDistinctCountResults(),
                    DistinctCountResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count inbox all stream", e);
        }
    }

    public DistinctCountResult countInboxStreamUnread(DistinctCountQuery query) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.callAndMerge(tenantId,
                    new MiruSolvableFactory<>(new CountInboxExecuteQuery(numberOfDistincts,
                            miruProvider.getBackfillerizer(tenantId), query, true)),
                    new DistinctCountResultEvaluator(query),
                    new MergeDistinctCountResults(),
                    DistinctCountResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count inbox unread stream", e);
        }
    }

    public DistinctCountResult countCustomStream(MiruPartitionId partitionId,
            DistinctCountQuery query,
            Optional<DistinctCountResult> lastResult)
            throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.callImmediate(tenantId,
                    partitionId,
                    new MiruSolvableFactory<>(new CountCustomExecuteQuery(numberOfDistincts, query)),
                    lastResult,
                    DistinctCountResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count custom stream for partition: " + partitionId.getId(), e);
        }
    }

    public DistinctCountResult countInboxStreamAll(MiruPartitionId partitionId,
            DistinctCountQuery query,
            Optional<DistinctCountResult> lastResult)
            throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.callImmediate(tenantId,
                    partitionId,
                    new MiruSolvableFactory<>(new CountInboxExecuteQuery(numberOfDistincts,
                            miruProvider.getBackfillerizer(tenantId), query, false)),
                    lastResult,
                    DistinctCountResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count inbox all stream for partition: " + partitionId.getId(), e);
        }
    }

    public DistinctCountResult countInboxStreamUnread(MiruPartitionId partitionId,
            DistinctCountQuery query,
            Optional<DistinctCountResult> lastResult)
            throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.callImmediate(tenantId,
                    partitionId,
                    new MiruSolvableFactory<>(new CountInboxExecuteQuery(numberOfDistincts,
                            miruProvider.getBackfillerizer(tenantId), query, true)),
                    lastResult,
                    DistinctCountResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count inbox unread stream for partition: " + partitionId.getId(), e);
        }
    }

}
