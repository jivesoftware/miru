package com.jivesoftware.os.miru.query;

import com.google.common.base.Optional;
import java.util.concurrent.Callable;

/**
 *
 */
public class MiruSolvableFactory<R, P> {

    private final ExecuteQuery<R, P> executeQuery;

    public MiruSolvableFactory(ExecuteQuery<R, P> executeQuery) {
        this.executeQuery = executeQuery;
    }

    public <BM> MiruSolvable<R> create(final MiruHostedPartition<BM> replica, final Optional<R> result) {
        Callable<R> callable = new Callable<R>() {
            @Override
            public R call() throws Exception {
                try (MiruQueryHandle<BM> handle = replica.getQueryHandle()) {
                    if (handle.isLocal()) {
                        return executeQuery.executeLocal(handle, executeQuery.createReport(result));
                    } else {
                        return executeQuery.executeRemote(handle.getRequestHelper(), handle.getCoord().partitionId, result);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
        return new MiruSolvable<>(replica.getCoord(), callable);
    }

    public String getQueryClass() {
        return executeQuery.getClass().getCanonicalName();
    }
}
