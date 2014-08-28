package com.jivesoftware.os.miru.query;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public interface Miru {

    <R, P> R callAndMerge(
            MiruTenantId tenantId,
            MiruSolvableFactory<R, P> solvableFactory,
            MiruResultEvaluator<R> evaluator,
            MiruResultMerger<R> merger,
            R defaultValue) throws Exception;

    <R, P> R callImmediate(
            MiruTenantId tenantId,
            MiruPartitionId partitionId,
            MiruSolvableFactory<R, P> factory,
            Optional<R> lastResult,
            R defaultValue) throws Exception;

}
