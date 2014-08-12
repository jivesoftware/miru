package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult;
import com.jivesoftware.os.miru.service.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.service.query.AggregateCountsReport;
import java.util.concurrent.Callable;

/**
 *
 */
public class AggregateCountsExecuteQueryCallableFactory implements ExecuteQueryCallableFactory<MiruHostedPartition, AggregateCountsResult> {

    private final ExecuteQuery<AggregateCountsResult, AggregateCountsReport> executeQuery;

    public AggregateCountsExecuteQueryCallableFactory(ExecuteQuery<AggregateCountsResult, AggregateCountsReport> executeQuery) {
        this.executeQuery = executeQuery;
    }

    @Override
    public MiruSolvable<AggregateCountsResult> create(final MiruHostedPartition replica, final Optional<AggregateCountsResult> result) {
        Callable<AggregateCountsResult> tracedCallable = new Callable<AggregateCountsResult>() {
            @Override
            public AggregateCountsResult call() throws Exception {
                return replica.createFilterCollector(executeQuery).collect(result);
            }
        };
        return new MiruSolvable<>(replica.getCoord(), tracedCallable, (Class<? extends ExecuteQuery<?, ?>>) executeQuery.getClass());
    }

    @Override
    public ExecuteQuery<AggregateCountsResult, AggregateCountsReport> getExecuteQuery() {
        return executeQuery;
    }
}
