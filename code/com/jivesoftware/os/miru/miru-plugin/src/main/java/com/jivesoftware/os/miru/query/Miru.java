package com.jivesoftware.os.miru.query;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.query.solution.MiruAnswerEvaluator;
import com.jivesoftware.os.miru.query.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.query.solution.MiruResponse;
import com.jivesoftware.os.miru.query.solution.MiruSolvableFactory;

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
            Optional<P> report,
            R defaultValue) throws Exception;

}
