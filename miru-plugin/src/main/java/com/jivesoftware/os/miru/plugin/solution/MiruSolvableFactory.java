package com.jivesoftware.os.miru.plugin.solution;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.partition.MiruQueryablePartition;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * @param <Q> query type
 * @param <A> answer type
 * @param <R> report type
 */
public class MiruSolvableFactory<Q, A, R> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruStats miruStats;
    private final String queryKey;
    private final Question<Q, A, R> question;

    public MiruSolvableFactory(MiruStats miruStats, String queryKey, Question<Q, A, R> question) {
        this.miruStats = miruStats;
        this.queryKey = queryKey;
        this.question = question;
    }

    public <BM extends IBM, IBM> MiruSolvable<A> create(final MiruQueryablePartition<BM, IBM> replica, final Optional<R> report) {
        Callable<MiruPartitionResponse<A>> callable = () -> {
            StackBuffer stackBuffer = new StackBuffer();
            try (MiruRequestHandle<BM, IBM, ?> handle = replica.acquireQueryHandle(stackBuffer)) {
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
                    miruStats.egressed(queryKey + ">remote>" + replica.getCoord().host.toStringForm(), 1, latency);
                    miruStats.egressed(queryKey + ">remote>" + replica.getCoord().tenantId.toString() + ">" + replica.getCoord().partitionId.getId(), 1,
                        latency);
                    return response;
                }
            } catch (MiruPartitionUnavailableException e) {
                LOG.info("Partition unavailable on {}: {}", replica.getCoord(), e.getMessage());
                throw e;
            } catch (InterruptedException ie) {
                LOG.debug("Solvable encountered an InterruptedException for {}", new Object[]{replica.getCoord()}, ie);
                throw ie;
            } catch (IOException io) {
                LOG.error("Solvable encountered an IOException for {}", new Object[]{replica.getCoord()}, io);
                throw io;
            } catch (Throwable t) {
                LOG.error("Solvable encountered a problem for {}", new Object[]{replica.getCoord()}, t);
                throw t;
            }

        };
        return new MiruSolvable<>(replica.getCoord(), callable);
    }

    public Question<Q, A, R> getQuestion() {
        return question;
    }

    public Optional<R> getReport(Optional<A> answer) {
        return question.createReport(answer);
    }

    public String getQueryKey() {
        return queryKey;
    }
}
