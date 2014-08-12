package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.service.partition.MiruHostedPartition;

/**
*
*/
public interface ExecuteQueryCallableFactory<M extends MiruHostedPartition, R> {

    MiruSolvable<R> create(M replica, Optional<R> result);

    ExecuteQuery<R, ?> getExecuteQuery();
}
