package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.query.result.TrendingResult;
import com.jivesoftware.os.miru.service.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.service.query.TrendingReport;
import java.util.concurrent.Callable;

/**
 *
 */
public class TrendingExecuteQueryCallableFactory implements ExecuteQueryCallableFactory<MiruHostedPartition, TrendingResult> {

    private final ExecuteQuery<TrendingResult, TrendingReport> executeQuery;

    public TrendingExecuteQueryCallableFactory(ExecuteQuery<TrendingResult, TrendingReport> executeQuery) {
        this.executeQuery = executeQuery;
    }

    @Override
    public MiruSolvable<TrendingResult> create(final MiruHostedPartition replica, final Optional<TrendingResult> result) {
        Callable<TrendingResult> tracedCallable = new Callable<TrendingResult>() {
            @Override
            public TrendingResult call() throws Exception {

                return replica.createTrendingCollector(executeQuery).collect(result);
            }
        };
        return new MiruSolvable<>(replica.getCoord(), tracedCallable, executeQuery.getClass().getCanonicalName());
    }

    @Override
    public ExecuteQuery<TrendingResult, TrendingReport> getExecuteQuery() {
        return executeQuery;
    }
}
