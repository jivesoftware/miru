package com.jivesoftware.os.miru.query;

import com.google.common.base.Optional;
import java.util.concurrent.Callable;

/**
 *
 */
public class MiruSolvableFactory<A, P> {

    private final String queryKey;
    private final Question<A, P> question;

    public MiruSolvableFactory(String queryKey, Question<A, P> question) {
        this.queryKey = queryKey;
        this.question = question;
    }

    public <BM> MiruSolvable<A> create(final MiruHostedPartition<BM> replica, final Optional<A> answer) {
        Callable<A> callable = new Callable<A>() {
            @Override
            public A call() throws Exception {
                try (MiruQueryHandle<BM> handle = replica.getQueryHandle()) {
                    if (handle.isLocal()) {
                        return question.askLocal(handle, question.createReport(answer));
                    } else {
                        return question.askRemote(handle.getRequestHelper(), handle.getCoord().partitionId, answer);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
        return new MiruSolvable<>(replica.getCoord(), callable);
    }

    public String getQueryKey() {
        return queryKey;
    }
}
