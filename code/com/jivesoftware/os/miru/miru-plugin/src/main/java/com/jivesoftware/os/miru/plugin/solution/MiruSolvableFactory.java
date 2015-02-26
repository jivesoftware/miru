package com.jivesoftware.os.miru.plugin.solution;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.plugin.partition.MiruHostedPartition;
import java.util.concurrent.Callable;

/**
 *
 * @param <A> answer type
 * @param <P> report type
 */
public class MiruSolvableFactory<A, P> {

    private final String queryKey;
    private final Question<A, P> question;

    public MiruSolvableFactory(String queryKey, Question<A, P> question) {
        this.queryKey = queryKey;
        this.question = question;
    }

    public <BM> MiruSolvable<A> create(final MiruHostedPartition<BM> replica, final Optional<P> report) {
        Callable<MiruPartitionResponse<A>> callable = new Callable<MiruPartitionResponse<A>>() {
            @Override
            public MiruPartitionResponse<A> call() throws Exception {
                try (MiruRequestHandle<BM> handle = replica.getQueryHandle()) {
                    if (handle.isLocal()) {
                        return question.askLocal(handle, report);
                    } else {
                        return question.askRemote(handle.getRequestHelper(), handle.getCoord().partitionId, report);
                    }
                } catch (Throwable t) {
                    System.out.println("Oh crap!");
                    throw t;
                }
            }
        };
        return new MiruSolvable<>(replica.getCoord(), callable);
    }

    public Question<A, P> getQuestion() {
        return question;
    }

    public Optional<P> getReport(Optional<A> answer) {
        return question.createReport(answer);
    }

    public String getQueryKey() {
        return queryKey;
    }
}
