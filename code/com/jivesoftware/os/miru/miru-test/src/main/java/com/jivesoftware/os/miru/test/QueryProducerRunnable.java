package com.jivesoftware.os.miru.test;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
*
*/
public class QueryProducerRunnable implements Runnable {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruTestQueryDistributor queryDistributor;
    private final BlockingQueue<Object> queue;
    private final AtomicBoolean done;
    private final Callable<?> callable;

    public QueryProducerRunnable(MiruTestQueryDistributor queryDistributor, BlockingQueue<Object> queue, AtomicBoolean done, Callable<?> callable) {
        this.queryDistributor = queryDistributor;
        this.queue = queue;
        this.done = done;
        this.callable = callable;
    }

    @Override
    public void run() {
        try {
            for (int i = 0; i < queryDistributor.getNumQueries(); i++) {
                queue.put(callable.call());
            }
        } catch (Exception e) {
            log.error("Query producer died", e);
        }

        done.compareAndSet(false, true);
    }
}
