package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.query.result.DistinctCountResult;
import com.jivesoftware.os.miru.service.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.service.query.DistinctCountReport;
import java.util.concurrent.Callable;

/**
 *
 */
public class DistinctCountExecuteQueryCallableFactory implements ExecuteQueryCallableFactory<MiruHostedPartition, DistinctCountResult> {

    private final ExecuteQuery<DistinctCountResult, DistinctCountReport> executeQuery;

    public DistinctCountExecuteQueryCallableFactory(ExecuteQuery<DistinctCountResult, DistinctCountReport> executeQuery) {
        this.executeQuery = executeQuery;
    }

    @Override
    public MiruSolvable<DistinctCountResult> create(final MiruHostedPartition replica, final Optional<DistinctCountResult> result) {
        Callable<DistinctCountResult> tracedCallable = new Callable<DistinctCountResult>() {
            @Override
            public DistinctCountResult call() throws Exception {
                return replica.createCountCollector(executeQuery).collect(result);
            }
        };
        return new MiruSolvable<>(replica.getCoord(), tracedCallable, (Class<ExecuteQuery<?, ?>>) executeQuery.getClass());
    }

    @Override
    public ExecuteQuery<DistinctCountResult, DistinctCountReport> getExecuteQuery() {
        return executeQuery;
    }
}
