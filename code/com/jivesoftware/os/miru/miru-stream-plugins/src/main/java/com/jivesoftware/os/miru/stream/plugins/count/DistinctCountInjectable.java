package com.jivesoftware.os.miru.stream.plugins.count;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.query.Miru;
import com.jivesoftware.os.miru.query.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.query.MiruProvider;
import com.jivesoftware.os.miru.query.MiruResponse;
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

    public MiruResponse<DistinctCountAnswer> countCustomStream(DistinctCountQuery query) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                    new MiruSolvableFactory<>("countCustomStream", new CountCustomQuestion(numberOfDistincts, query)),
                    new DistinctCountAnswerEvaluator(query),
                    new DistinctCounterAnswerMerger(),
                    DistinctCountAnswer.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count custom stream", e);
        }
    }

    public MiruResponse<DistinctCountAnswer> countInboxStreamAll(DistinctCountQuery query) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                    new MiruSolvableFactory<>("countInboxStreamAll", new CountInboxQuestion(numberOfDistincts,
                            miruProvider.getBackfillerizer(tenantId), query, false)),
                    new DistinctCountAnswerEvaluator(query),
                    new DistinctCounterAnswerMerger(),
                    DistinctCountAnswer.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count inbox all stream", e);
        }
    }

    public MiruResponse<DistinctCountAnswer> countInboxStreamUnread(DistinctCountQuery query) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                    new MiruSolvableFactory<>("countInboxStreamUnread", new CountInboxQuestion(numberOfDistincts,
                            miruProvider.getBackfillerizer(tenantId), query, true)),
                    new DistinctCountAnswerEvaluator(query),
                    new DistinctCounterAnswerMerger(),
                    DistinctCountAnswer.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count inbox unread stream", e);
        }
    }

    public DistinctCountAnswer countCustomStream(MiruPartitionId partitionId,
            DistinctCountQueryAndResult queryAndResult)
            throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = queryAndResult.query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                    partitionId,
                    new MiruSolvableFactory<>("countCustomStream", new CountCustomQuestion(numberOfDistincts, queryAndResult.query)),
                    Optional.fromNullable(queryAndResult.lastResult),
                    DistinctCountAnswer.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count custom stream for partition: " + partitionId.getId(), e);
        }
    }

    public DistinctCountAnswer countInboxStreamAll(MiruPartitionId partitionId,
            DistinctCountQueryAndResult queryAndResult)
            throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = queryAndResult.query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                    partitionId,
                    new MiruSolvableFactory<>("countInboxStreamAll", new CountInboxQuestion(numberOfDistincts,
                            miruProvider.getBackfillerizer(tenantId), queryAndResult.query, false)),
                    Optional.fromNullable(queryAndResult.lastResult),
                    DistinctCountAnswer.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count inbox all stream for partition: " + partitionId.getId(), e);
        }
    }

    public DistinctCountAnswer countInboxStreamUnread(MiruPartitionId partitionId,
            DistinctCountQueryAndResult queryAndResult)
            throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = queryAndResult.query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                    partitionId,
                    new MiruSolvableFactory<>("countInboxStreamUnread", new CountInboxQuestion(numberOfDistincts,
                            miruProvider.getBackfillerizer(tenantId), queryAndResult.query, true)),
                    Optional.fromNullable(queryAndResult.lastResult),
                    DistinctCountAnswer.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count inbox unread stream for partition: " + partitionId.getId(), e);
        }
    }

}
