package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.jivesoftware.jive.tracing_api.Annotation;
import com.jivesoftware.jive.tracing_api.Span;
import com.jivesoftware.jive.tracing_api.Trace;
import com.jivesoftware.jive.tracing_api.Wrapable;
import com.jivesoftware.jive.tracing_api.common.TracedCallable;
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
        TracedCallable<TrendingResult> tracedCallable = TracedCallable.decorate(new Callable<TrendingResult>() {
            @Override
            public TrendingResult call() throws Exception {
                try (Span child = Trace.currentSpan().child().name("TrendingResult - " + (replica.isLocal() ? "Local" : "Remote"));
                    Trace ignore = Trace.unwind(child);
                    Wrapable ignored = child.wrap(Annotation.asyncStart())) {
                    child.classify("partitionId", replica.getPartitionId());

                    return replica.createTrendingCollector(executeQuery).collect(result);
                }
            }
        });
        return new MiruSolvable<>(replica.getCoord(), tracedCallable, (Class<ExecuteQuery<?, ?>>) executeQuery.getClass());
    }

    @Override
    public ExecuteQuery<TrendingResult, TrendingReport> getExecuteQuery() {
        return executeQuery;
    }
}
