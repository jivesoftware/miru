package com.jivesoftware.os.miru.service.stream.factory.local;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult;
import com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition;
import com.jivesoftware.os.miru.service.query.AggregateCountsReport;
import com.jivesoftware.os.miru.service.stream.factory.ExecuteQuery;
import com.jivesoftware.os.miru.service.stream.factory.MiruStreamCollector;

/**
 *
 */
public class LocalFilterCollector implements MiruStreamCollector<AggregateCountsResult> {

    private final MiruLocalHostedPartition replica;
    private final ExecuteQuery<AggregateCountsResult, AggregateCountsReport> executeQuery;

    public LocalFilterCollector(MiruLocalHostedPartition replica,
        ExecuteQuery<AggregateCountsResult, AggregateCountsReport> executeQuery) {
        this.replica = replica;
        this.executeQuery = executeQuery;
    }

    @Override
    public AggregateCountsResult collect(final Optional<AggregateCountsResult> result) throws Exception {
        Optional<AggregateCountsReport> report = Optional.absent();
        if (result.isPresent()) {
            report = Optional.of(new AggregateCountsReport(
                result.get().skippedDistincts,
                result.get().collectedDistincts,
                result.get().aggregateTerms));
        }
        return executeQuery.executeLocal(replica, report);
    }

}
