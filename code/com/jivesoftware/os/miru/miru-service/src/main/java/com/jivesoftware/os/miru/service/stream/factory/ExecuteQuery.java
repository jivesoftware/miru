package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition;
import com.jivesoftware.os.miru.service.partition.MiruRemoteHostedPartition;

/**
 *
 * @author jonathan
 * @param <R>
 * @param <P>
 */
public interface ExecuteQuery<R, P> {

    R executeLocal(MiruLocalHostedPartition partition, Optional<P> report) throws Exception;

    R executeRemote(MiruRemoteHostedPartition partition, Optional<R> lastResult) throws Exception;
}
