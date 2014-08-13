package com.jivesoftware.os.miru.service.stream.factory.local;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.query.result.RecoResult;
import com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition;
import com.jivesoftware.os.miru.service.query.RecoReport;
import com.jivesoftware.os.miru.service.stream.factory.ExecuteQuery;
import com.jivesoftware.os.miru.service.stream.factory.MiruStreamCollector;

/**
 *
 */
public class LocalRecoCollector implements MiruStreamCollector<RecoResult> {

    private final MiruLocalHostedPartition replica;
    private final ExecuteQuery<RecoResult, RecoReport> executeQuery;

    public LocalRecoCollector(MiruLocalHostedPartition replica, ExecuteQuery<RecoResult, RecoReport> executeQuery) {
        this.replica = replica;
        this.executeQuery = executeQuery;
    }

    @Override
    public RecoResult collect(final Optional<RecoResult> result) throws Exception {
        Optional<RecoReport> report = Optional.absent();
        if (result.isPresent()) {
            report = Optional.of(new RecoReport());
        }
        return executeQuery.executeLocal(replica, report);
    }

}
