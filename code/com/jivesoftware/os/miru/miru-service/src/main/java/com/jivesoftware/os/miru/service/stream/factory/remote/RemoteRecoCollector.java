package com.jivesoftware.os.miru.service.stream.factory.remote;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.query.result.RecoResult;
import com.jivesoftware.os.miru.service.partition.MiruRemoteHostedPartition;
import com.jivesoftware.os.miru.service.query.RecoReport;
import com.jivesoftware.os.miru.service.stream.factory.ExecuteQuery;
import com.jivesoftware.os.miru.service.stream.factory.MiruStreamCollector;

public class RemoteRecoCollector implements MiruStreamCollector<RecoResult> {

    private final MiruRemoteHostedPartition replica;
    private final ExecuteQuery<RecoResult, RecoReport> executeQuery;

    public RemoteRecoCollector(MiruRemoteHostedPartition replica, ExecuteQuery<RecoResult, RecoReport> executeQuery) {
        this.replica = replica;
        this.executeQuery = executeQuery;
    }

    @Override
    public RecoResult collect(Optional<RecoResult> result) throws Exception {
        return executeQuery.executeRemote(replica, result);
    }

}
