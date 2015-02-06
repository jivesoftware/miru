package com.jivesoftware.os.miru.metric.sampler;

import com.jivesoftware.os.mlogger.core.AtomicCounter;
import com.jivesoftware.os.mlogger.core.Counter;
import com.jivesoftware.os.mlogger.core.CountersAndTimers;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.Timer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class HttpMiruMetricSampler implements MiruMetricSampler, Runnable {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String datacenter;
    private final String cluster;
    private final String host;
    private final String service;
    private final String instance;
    private final String version;
    private final AtomicLong senderIndex = new AtomicLong();
    private final MiruMetricSampleSender[] sender;
    private final int maxBacklog;
    private final int sampleIntervalInMillis;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ScheduledExecutorService sampler = Executors.newSingleThreadScheduledExecutor();

    public HttpMiruMetricSampler(String datacenter,
        String cluster,
        String host,
        String service,
        String instance,
        String version,
        MiruMetricSampleSender[] logSenders,
        int sampleIntervalInMillis,
        int maxBacklog) {
        this.datacenter = datacenter;
        this.host = host;
        this.service = service;
        this.instance = instance;
        this.version = version;
        this.sender = logSenders;
        this.cluster = cluster;
        this.sampleIntervalInMillis = sampleIntervalInMillis;
        this.maxBacklog = maxBacklog;
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            sampler.scheduleAtFixedRate(this, sampleIntervalInMillis, sampleIntervalInMillis, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            sampler.shutdownNow();
        }
    }

    @Override
    public boolean isStarted() {
        return running.get();
    }

    @Override
    public void run() {

        List<AnomalyMetric> samples = new ArrayList<>();

        while (running.get()) {
            samples.addAll(sample());
            send(samples);
            try {
                samples.clear();
                Thread.sleep(sampleIntervalInMillis); // expose to config
            } catch (InterruptedException e) {
                log.warn("Sender was interrupted while sleeping due to errors");
                Thread.interrupted();
                break;
            }

        }

    }

    private List<AnomalyMetric> sample() {
        String time = String.valueOf(System.currentTimeMillis());
        List<AnomalyMetric> metrics = new ArrayList<>();
        for (CountersAndTimers a : CountersAndTimers.getAll()) {
            for (Entry<String, Counter> counter : a.getCounters()) {
                metrics.add(new AnomalyMetric(
                    datacenter,
                    cluster,
                    host,
                    service,
                    instance,
                    version,
                    a.getLoggerName(),
                    counter.getKey().split("\\>"),
                    "counter",
                    counter.getValue().getCount(),
                    time));
            }

            for (Entry<String, AtomicCounter> atomicCounter : a.getAtomicCounters()) {
                metrics.add(new AnomalyMetric(
                    datacenter,
                    cluster,
                    host,
                    service,
                    instance,
                    version,
                    a.getLoggerName(),
                    atomicCounter.getKey().split("\\>"),
                    "atomicCounter",
                    atomicCounter.getValue().getCount(),
                    time));
            }

            for (Entry<String, Timer> timers : a.getTimers()) {
                metrics.add(new AnomalyMetric(
                    datacenter,
                    cluster,
                    host,
                    service,
                    instance,
                    version,
                    a.getLoggerName(),
                    timers.getKey().split("\\>"),
                    "timer",
                    timers.getValue().getLastSample(),
                    time));
            }
        }
        return metrics;
    }

    private void send(List<AnomalyMetric> samples) {
        for (MiruMetricSampleSender s : sender) {
            int i = 0;
            try {
                i = (int) (senderIndex.get() % sender.length);
                sender[i].send(samples);
                samples.clear();
                return;
            } catch (Exception e) {
                log.warn("Sampler:" + sender[i] + " failed to send:" + samples.size() + " samples.", e);
                senderIndex.incrementAndGet();
            }
        }
    }

}
