package com.jivesoftware.os.miru.service.stream.factory.remote;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.query.result.DistinctCountResult;
import com.jivesoftware.os.miru.service.partition.MiruRemoteHostedPartition;
import com.jivesoftware.os.miru.service.query.DistinctCountReport;
import com.jivesoftware.os.miru.service.stream.factory.ExecuteQuery;
import com.jivesoftware.os.miru.service.stream.factory.MiruStreamCollector;

public class RemoteCountCollector implements MiruStreamCollector<DistinctCountResult> {

    private final MiruRemoteHostedPartition replica;
    private final ExecuteQuery<DistinctCountResult, DistinctCountReport> executeQuery;

    public RemoteCountCollector(MiruRemoteHostedPartition replica, ExecuteQuery<DistinctCountResult, DistinctCountReport> executeQuery) {
        this.replica = replica;
        this.executeQuery = executeQuery;
    }

    @Override
    public DistinctCountResult collect(Optional<DistinctCountResult> result) throws Exception {
        return executeQuery.executeRemote(replica, result);
    }

}
