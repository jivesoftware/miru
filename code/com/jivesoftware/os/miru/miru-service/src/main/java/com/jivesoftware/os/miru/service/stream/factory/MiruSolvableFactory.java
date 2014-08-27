package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.service.partition.MiruQueryHandle;

import java.util.concurrent.Callable;

/**
 *
 */
public class MiruSolvableFactory<R, P> {

    private final ExecuteQuery<R, P> executeQuery;

    public MiruSolvableFactory(ExecuteQuery<R, P> executeQuery) {
        this.executeQuery = executeQuery;
    }

    public MiruSolvable<R> create(final MiruQueryHandle handle, final Optional<R> result) {
        Callable<R> callable = new Callable<R>() {
            @Override
            public R call() throws Exception {
                if (handle.isLocal()) {
                    return executeQuery.executeLocal(handle, executeQuery.createReport(result));
                } else {
                    return executeQuery.executeRemote(handle.getRequestHelper(), handle.getCoord().partitionId, result);
                }
            }
        };
        return new MiruSolvable<>(handle.getCoord(), callable);
    }

    public String getQueryClass() {
        return executeQuery.getClass().getCanonicalName();
    }
}
