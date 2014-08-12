package com.jivesoftware.os.miru.service.stream.factory.remote;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.query.result.TrendingResult;
import com.jivesoftware.os.miru.service.partition.MiruRemoteHostedPartition;
import com.jivesoftware.os.miru.service.query.TrendingReport;
import com.jivesoftware.os.miru.service.stream.factory.ExecuteQuery;
import com.jivesoftware.os.miru.service.stream.factory.MiruStreamCollector;

public class RemoteTrendingCollector implements MiruStreamCollector<TrendingResult> {

    private final MiruRemoteHostedPartition replica;
    private final ExecuteQuery<TrendingResult, TrendingReport> executeQuery;

    public RemoteTrendingCollector(MiruRemoteHostedPartition replica, ExecuteQuery<TrendingResult, TrendingReport> executeQuery) {
        this.replica = replica;
        this.executeQuery = executeQuery;
    }

    @Override
    public TrendingResult collect(Optional<TrendingResult> result) throws Exception {
        return executeQuery.executeRemote(replica, result);
    }

}
