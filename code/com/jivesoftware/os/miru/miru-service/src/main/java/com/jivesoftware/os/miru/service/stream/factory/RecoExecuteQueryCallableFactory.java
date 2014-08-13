package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.query.result.RecoResult;
import com.jivesoftware.os.miru.service.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.service.query.RecoReport;
import java.util.concurrent.Callable;

/**
 *
 */
public class RecoExecuteQueryCallableFactory implements ExecuteQueryCallableFactory<MiruHostedPartition, RecoResult> {

    private final ExecuteQuery<RecoResult, RecoReport> executeQuery;

    public RecoExecuteQueryCallableFactory(ExecuteQuery<RecoResult, RecoReport> executeQuery) {
        this.executeQuery = executeQuery;
    }

    @Override
    public MiruSolvable<RecoResult> create(final MiruHostedPartition replica, final Optional<RecoResult> result) {
        Callable<RecoResult> tracedCallable = new Callable<RecoResult>() {
            @Override
            public RecoResult call() throws Exception {

                return replica.createRecoCollector(executeQuery).collect(result);
            }
        };
        return new MiruSolvable<>(replica.getCoord(), tracedCallable, (Class<ExecuteQuery<?, ?>>) executeQuery.getClass());
    }

    @Override
    public ExecuteQuery<RecoResult, RecoReport> getExecuteQuery() {
        return executeQuery;
    }
}
