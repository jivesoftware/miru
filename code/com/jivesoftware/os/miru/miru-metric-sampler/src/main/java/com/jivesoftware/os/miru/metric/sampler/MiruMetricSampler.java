package com.jivesoftware.os.miru.metric.sampler;

import com.jivesoftware.os.miru.metric.sampler.MiruMetricSampleEvent.MetricKey;
import com.jivesoftware.os.mlogger.core.AtomicCounter;
import com.jivesoftware.os.mlogger.core.Counter;
import com.jivesoftware.os.mlogger.core.CountersAndTimers;
import com.jivesoftware.os.mlogger.core.Timer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class MiruMetricSampler implements Runnable {

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

    public MiruMetricSampler(String datacenter,
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

    public void start() {
        if (running.compareAndSet(false, true)) {
            sampler.scheduleAtFixedRate(this, sampleIntervalInMillis, sampleIntervalInMillis, TimeUnit.MILLISECONDS);
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            sampler.shutdownNow();
        }
    }

    public boolean isStarted() {
        return running.get();
    }

    @Override
    public void run() {

        MiruMetricSampleEvent miruLogEvent = sample();

        List<MiruMetricSampleEvent> samples = new ArrayList<>();

        while (running.get()) {
            samples.add(sample());
            while (samples.size() > maxBacklog) {
                samples.remove(0);
            }
            send(samples);
            try {
                Thread.sleep(sampleIntervalInMillis); // expose to config
            } catch (InterruptedException e) {
                System.err.println("Sender was interrupted while sleeping due to errors");
                Thread.interrupted();
                break;
            }
        }

    }

    private MiruMetricSampleEvent sample() {
        Map<MetricKey, Long> metricAndValue = new HashMap<>();
        for (CountersAndTimers a : CountersAndTimers.getAll()) {
            for (Entry<String, Counter> counter : a.getCounters()) {
                metricAndValue.put(new MetricKey(a.getLoggerName(), counter.getKey().split("\\>"), "timer"), counter.getValue().getCount());
            }

            for (Entry<String, AtomicCounter> atomicCounter : a.getAtomicCounters()) {
                metricAndValue.put(new MetricKey(a.getLoggerName(), atomicCounter.getKey().split("\\>"), "atomicCounter"), atomicCounter.getValue().getCount());
            }

            for (Entry<String, Timer> timers : a.getTimers()) {
                metricAndValue.put(new MetricKey(a.getLoggerName(), timers.getKey().split("\\>"), "timer"), timers.getValue().getLastSample());
            }
        }
        MiruMetricSampleEvent miruLogEvent = new MiruMetricSampleEvent(datacenter,
            cluster,
            host,
            service,
            instance,
            version,
            metricAndValue,
            String.valueOf(System.currentTimeMillis()));
        return miruLogEvent;
    }

    private void send(List<MiruMetricSampleEvent> samples) {
        for (int tries = 0; tries < sender.length; tries++) {
            int i = 0;
            try {
                i = (int) (senderIndex.get() % sender.length);
                sender[i].send(samples);
                samples.clear();
                return;
            } catch (Exception e) {
                System.err.println("Sampler:" + sender[i] + " failed to send:" + samples.size() + " samples.");
                senderIndex.incrementAndGet();
            }
        }
    }

}
