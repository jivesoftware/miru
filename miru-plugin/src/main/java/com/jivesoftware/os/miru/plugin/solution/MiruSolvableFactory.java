package com.jivesoftware.os.miru.plugin.solution;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.partition.MiruQueryablePartition;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.InterruptedIOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.Callable;

/**
 * @param <Q> query type
 * @param <A> answer type
 * @param <R> report type
 */
public class MiruSolvableFactory<Q, A, R> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String requestName;
    private final MiruStats miruStats;
    private final String queryKey;
    private final Question<Q, A, R> question;

    public MiruSolvableFactory(String requestName, MiruStats miruStats, String queryKey, Question<Q, A, R> question) {
        this.requestName = requestName;
        this.miruStats = miruStats;
        this.queryKey = queryKey;
        this.question = question;
    }

    public <BM extends IBM, IBM> MiruSolvable<A> create(final MiruQueryablePartition<BM, IBM> replica, final Optional<R> report, MiruSolutionLog solutionLog) {
        Callable<MiruPartitionResponse<A>> callable = () -> {
            try (MiruRequestHandle<BM, IBM, ?> handle = replica.acquireQueryHandle()) {
                if (handle.isLocal()) {
                    long start = System.currentTimeMillis();
                    MiruPartitionResponse<A> response = question.askLocal(handle, report);
                    long latency = System.currentTimeMillis() - start;
                    miruStats.egressed(queryKey + ">local", 1, latency);
                    miruStats.egressed(queryKey + ">local>" + replica.getCoord().tenantId.toString() + ">" + replica.getCoord().partitionId.getId(), 1,
                        latency);
                    return response;
                } else {
                    long start = System.currentTimeMillis();
                    MiruPartitionResponse<A> response = question.askRemote(handle.getCoord().host, handle.getCoord().partitionId, report);
                    long latency = System.currentTimeMillis() - start;
                    miruStats.egressed(queryKey + ">remote", 1, latency);
                    miruStats.egressed(queryKey + ">remote>" + replica.getCoord().host.getLogicalName(), 1, latency);
                    miruStats.egressed(queryKey + ">remote>" + replica.getCoord().tenantId.toString() + ">" + replica.getCoord().partitionId.getId(), 1,
                        latency);
                    return response;
                }
            } catch (MiruPartitionUnavailableException e) {
                LOG.info("Partition unavailable on {} {} {}: {}", requestName, queryKey, replica.getCoord(), e.getMessage());
                throw e;
            } catch (Throwable t) {
                Throwable cause = t;
                for (int i = 0; i < 10 && cause != null; i++) {
                    if (cause instanceof InterruptedException || cause instanceof InterruptedIOException || cause instanceof ClosedByInterruptException) {
                        LOG.debug("Solvable interrupted for {} {} {}", new Object[] { requestName, queryKey, replica.getCoord() }, t);
                        throw t;
                    }
                    cause = cause.getCause();
                }

                LOG.error("Solvable failed for {} {} {}", new Object[] { requestName, queryKey, replica.getCoord() }, t);
                throw t;
            }

        };
        return new MiruSolvable<>(replica.getCoord(), callable, replica.isLocal(), solutionLog);
    }

    public Question<Q, A, R> getQuestion() {
        return question;
    }

    public Optional<R> getReport(Optional<A> answer) {
        return question.createReport(answer);
    }

    public String getRequestName() {
        return requestName;
    }

    public String getQueryKey() {
        return queryKey;
    }
}
