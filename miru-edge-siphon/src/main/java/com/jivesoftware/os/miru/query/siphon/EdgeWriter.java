package com.jivesoftware.os.miru.query.siphon;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by jonathan.colt on 5/4/17.
 */
public class EdgeWriter {

    public static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final PartitionClientProvider clientProvider;
    private final OrderIdProvider orderIdProvider;
    private final String partitionName;
    private final PartitionProperties partitionProperties;
    private final AtomicReference<List<Edge>> edges = new AtomicReference<>(Collections.synchronizedList(Lists.newArrayList()));
    private final Semaphore semaphore = new Semaphore(Short.MAX_VALUE);
    private final long autoFlushAtIntervalMillis;
    private final int autoFlushAtCapacity;
    private final ObjectMapper mapper;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<ScheduledExecutorService> scheduledExecutorService = new AtomicReference<>();

    public EdgeWriter(PartitionClientProvider clientProvider,
        OrderIdProvider orderIdProvider,
        String partitionName,
        long ttlMillis,
        long autoFlushAtIntervalMillis,
        int autoFlushAtCapacity,
        ObjectMapper mapper) {

        this.clientProvider = clientProvider;
        this.orderIdProvider = orderIdProvider;
        this.partitionName = partitionName;
        this.partitionProperties = new PartitionProperties(Durability.fsync_async,
            0, 0, 0, 0,
            ttlMillis, ttlMillis / 2, ttlMillis, ttlMillis / 2,
            false, Consistency.none, true, true, false, RowType.primary, "lab", -1, null, -1, -1);
        this.autoFlushAtIntervalMillis = autoFlushAtIntervalMillis;
        this.autoFlushAtCapacity = autoFlushAtCapacity;


        this.mapper = mapper;
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
            scheduledExecutorService.set(executorService);

            executorService.scheduleWithFixedDelay(() -> {
                try {
                    flush();
                } catch (Exception x) {
                    LOG.warn("Flushing " + partitionName + "failure.", x);
                }
            }, autoFlushAtIntervalMillis, autoFlushAtIntervalMillis, TimeUnit.MILLISECONDS);
        }
    }

    public void stop() {
        ScheduledExecutorService executorService = scheduledExecutorService.get();
        if (running.compareAndSet(true, false)) {
            if (scheduledExecutorService.compareAndSet(executorService, null)) {
                executorService.shutdownNow();
            }
        }
    }

    public void write(
        String tenant,
        String user,
        String name,
        String origin,
        String destination,
        long latency,
        String... tags
    ) throws Exception {

        semaphore.acquire();
        try {
            List<Edge> list = edges.get();
            list.add(new Edge(orderIdProvider.nextId(), System.currentTimeMillis(), tenant, user, name, origin, destination, Sets.newHashSet(tags), latency));
            if (list.size() > autoFlushAtCapacity) {
                ScheduledExecutorService executorService = scheduledExecutorService.get();
                executorService.schedule(() -> {
                    try {
                        flush();
                    } catch (Exception x) {
                        LOG.warn("Flushing " + partitionName + "failure.", x);
                    }
                }, 0, TimeUnit.MILLISECONDS);
            }
        } finally {
            semaphore.release();
        }
    }


    public void flush() throws Exception {
        List<Edge> flush = null;
        semaphore.acquire(Short.MAX_VALUE);
        try {
            List<Edge> list = edges.get();
            if (!list.isEmpty()) {
                flush = list;
                edges.set(Collections.synchronizedList(Lists.newArrayList()));
            }

        } finally {
            semaphore.release(Short.MAX_VALUE);
        }

        if (flush != null) {
            LOG.inc(partitionName + ">flushed", flush.size());
            List<Edge> flushable = flush;
            client().commit(Consistency.none,
                null,
                commitKeyValueStream -> {
                    for (Edge e : flushable) {
                        commitKeyValueStream.commit(UIO.longBytes(e.id), mapper.writeValueAsBytes(e), -1, false);
                    }
                    return true;
                },
                10_000,
                30_000,
                Optional.empty());
        }
    }

    private PartitionClient client() throws Exception {
        return clientProvider.getPartition(edgePartition(partitionName), 3, partitionProperties);
    }

    private PartitionName edgePartition(String name) {
        byte[] nameBytes = ("edge-" + name).getBytes(StandardCharsets.UTF_8);
        return new PartitionName(false, nameBytes, nameBytes);
    }

}
