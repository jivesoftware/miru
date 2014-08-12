package com.jivesoftware.os.miru.service.stream.factory.remote;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult;
import com.jivesoftware.os.miru.service.partition.MiruRemoteHostedPartition;
import com.jivesoftware.os.miru.service.query.AggregateCountsReport;
import com.jivesoftware.os.miru.service.stream.factory.ExecuteQuery;
import com.jivesoftware.os.miru.service.stream.factory.MiruStreamCollector;

public class RemoteFilterCollector implements MiruStreamCollector<AggregateCountsResult> {

    private final MiruRemoteHostedPartition replica;
    private final ExecuteQuery<AggregateCountsResult, AggregateCountsReport> executeQuery;

    public RemoteFilterCollector(MiruRemoteHostedPartition replica, ExecuteQuery<AggregateCountsResult, AggregateCountsReport> executeQuery) {
        this.replica = replica;
        this.executeQuery = executeQuery;
    }

    @Override
    public AggregateCountsResult collect(Optional<AggregateCountsResult> result) throws Exception {
        return executeQuery.executeRemote(replica, result);
    }

}
