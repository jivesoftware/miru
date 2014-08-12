package com.jivesoftware.os.miru.service.stream.factory.local;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.query.result.TrendingResult;
import com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition;
import com.jivesoftware.os.miru.service.query.TrendingReport;
import com.jivesoftware.os.miru.service.stream.factory.ExecuteQuery;
import com.jivesoftware.os.miru.service.stream.factory.MiruStreamCollector;

/**
 *
 */
public class LocalTrendingCollector implements MiruStreamCollector<TrendingResult> {

    private final MiruLocalHostedPartition replica;
    private final ExecuteQuery<TrendingResult, TrendingReport> executeQuery;

    public LocalTrendingCollector(MiruLocalHostedPartition replica, ExecuteQuery<TrendingResult, TrendingReport> executeQuery) {
        this.replica = replica;
        this.executeQuery = executeQuery;
    }

    @Override
    public TrendingResult collect(final Optional<TrendingResult> result) throws Exception {
        Optional<TrendingReport> report = Optional.absent();
        if (result.isPresent()) {
            report = Optional.of(new TrendingReport(
                result.get().collectedDistincts,
                result.get().aggregateTerms));
        }
        return executeQuery.executeLocal(replica, report);
    }

}
