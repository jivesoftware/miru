package com.jivesoftware.os.miru.query;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public interface Miru {

    <R, P> MiruResponse<R> askAndMerge(
            MiruTenantId tenantId,
            MiruSolvableFactory<R, P> solvableFactory,
            MiruAnswerEvaluator<R> evaluator,
            MiruAnswerMerger<R> merger,
            R defaultValue) throws Exception;

    <R, P> R askImmediate(
            MiruTenantId tenantId,
            MiruPartitionId partitionId,
            MiruSolvableFactory<R, P> factory,
            Optional<R> lastResult,
            R defaultValue) throws Exception;

}
