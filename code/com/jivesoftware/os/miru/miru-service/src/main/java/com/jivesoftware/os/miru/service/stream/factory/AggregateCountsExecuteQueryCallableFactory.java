package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult;
import com.jivesoftware.os.miru.service.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.service.query.AggregateCountsReport;
import com.jivesoftware.jive.tracing_api.Annotation;
import com.jivesoftware.jive.tracing_api.Span;
import com.jivesoftware.jive.tracing_api.Trace;
import com.jivesoftware.jive.tracing_api.Wrapable;
import com.jivesoftware.jive.tracing_api.common.TracedCallable;
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
        TracedCallable<AggregateCountsResult> tracedCallable = TracedCallable.decorate(new Callable<AggregateCountsResult>() {
            @Override
            public AggregateCountsResult call() throws Exception {
                try (Span child = Trace.currentSpan().child().name("AggregateCount - " + (replica.isLocal() ? "Local" : "Remote"));
                    Trace ignore = Trace.unwind(child);
                    Wrapable ignored = child.wrap(Annotation.asyncStart())) {
                    child.classify("partitionId", replica.getPartitionId());

                    return replica.createFilterCollector(executeQuery).collect(result);
                }
            }
        });
        return new MiruSolvable<>(replica.getCoord(), tracedCallable, (Class<? extends ExecuteQuery<?, ?>>) executeQuery.getClass());
    }

    @Override
    public ExecuteQuery<AggregateCountsResult, AggregateCountsReport> getExecuteQuery() {
        return executeQuery;
    }
}
