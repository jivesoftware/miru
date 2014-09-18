package com.jivesoftware.os.miru.plugin;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerEvaluator;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolvableFactory;

/**
 *
 */
public interface Miru {

    <R, P> MiruResponse<R> askAndMerge(
        MiruTenantId tenantId, MiruSolvableFactory<R, P> solvableFactory,
        MiruAnswerEvaluator<R> evaluator,
        MiruAnswerMerger<R> merger,
        R defaultValue,
        boolean debug) throws Exception;

    <R, P> MiruPartitionResponse<R> askImmediate(
        MiruTenantId tenantId,
        MiruPartitionId partitionId,
        MiruSolvableFactory<R, P> factory, Optional<P> report,
        R defaultValue,
        boolean debug) throws Exception;

}
