package com.jivesoftware.os.miru.service.stream.factory.local;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.query.result.DistinctCountResult;
import com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition;
import com.jivesoftware.os.miru.service.query.DistinctCountReport;
import com.jivesoftware.os.miru.service.stream.factory.ExecuteQuery;
import com.jivesoftware.os.miru.service.stream.factory.MiruStreamCollector;

/**
 *
 */
public class LocalCountCollector implements MiruStreamCollector<DistinctCountResult> {

    private final MiruLocalHostedPartition replica;
    private final ExecuteQuery<DistinctCountResult, DistinctCountReport> executeQuery;

    public LocalCountCollector(MiruLocalHostedPartition replica,
        ExecuteQuery<DistinctCountResult, DistinctCountReport> executeQuery) {
        this.replica = replica;
        this.executeQuery = executeQuery;
    }

    @Override
    public DistinctCountResult collect(final Optional<DistinctCountResult> result) throws Exception {
        Optional<DistinctCountReport> report = Optional.absent();
        if (result.isPresent()) {
            report = Optional.of(new DistinctCountReport(
                result.get().collectedDistincts,
                result.get().aggregateTerms));
        }
        return executeQuery.executeLocal(replica, report);
    }

}
