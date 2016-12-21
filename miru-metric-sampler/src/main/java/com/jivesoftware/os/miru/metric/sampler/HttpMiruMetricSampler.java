package com.jivesoftware.os.miru.metric.sampler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.mlogger.core.AtomicCounter;
import com.jivesoftware.os.mlogger.core.Counter;
import com.jivesoftware.os.mlogger.core.CountersAndTimers;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.Timer;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.routing.bird.shared.NextClientStrategy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class HttpMiruMetricSampler implements MiruMetricSampler, Runnable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String datacenter;
    private final String cluster;
    private final String host;
    private final String service;
    private final String instance;
    private final String version;
    private final TenantAwareHttpClient<String> client;
    private final int sampleIntervalInMillis;
    private final boolean tenantLevelMetricsEnable;
    private final boolean jvmMetricsEnabled;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ScheduledExecutorService sampler = Executors.newSingleThreadScheduledExecutor();
    private final JVMMetrics jvmMetrics;

    private final NextClientStrategy nextClientStrategy = new RoundRobinStrategy();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final String endpoint = "/miru/anomaly/intake";

    public HttpMiruMetricSampler(String datacenter,
        String cluster,
        String host,
        String service,
        String instance,
        String version,
        TenantAwareHttpClient<String> client,
        int sampleIntervalInMillis,
        boolean tenantLevelMetricsEnable,
        boolean jvmMetricsEnabled) {
        this.datacenter = datacenter;
        this.host = host;
        this.service = service;
        this.instance = instance;
        this.version = version;
        this.client = client;
        this.cluster = cluster;
        this.sampleIntervalInMillis = sampleIntervalInMillis;
        this.tenantLevelMetricsEnable = tenantLevelMetricsEnable;
        this.jvmMetricsEnabled = jvmMetricsEnabled;
        if (this.jvmMetricsEnabled) {
            this.jvmMetrics = new JVMMetrics();
        } else {
            this.jvmMetrics = null;
        }
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

            try {
                String toJson = objectMapper.writeValueAsString(samples);

                HttpResponse httpResponse = client.call(
                    "",
                    nextClientStrategy,
                    "ingress",
                    client -> new ClientCall.ClientResponse<>(client.postJson(endpoint, toJson, null), true));
                if (httpResponse.getStatusCode() != 200) {
                    LOG.warn("Error posting: {}", toJson);
                }
            } catch (Exception e) {
                LOG.warn("Error posting: {}", endpoint);
            }

            try {
                samples.clear();
                Thread.sleep(sampleIntervalInMillis); // expose to config
            } catch (InterruptedException e) {
                LOG.warn("Sender was interrupted while sleeping due to errors");
                Thread.interrupted();
                break;
            }
        }
    }

    private List<AnomalyMetric> sample() {
        String time = String.valueOf(System.currentTimeMillis());
        List<AnomalyMetric> metrics = new ArrayList<>();
        for (CountersAndTimers a : CountersAndTimers.getAll()) {
            gather("null", a, metrics, time);
            if (tenantLevelMetricsEnable) {
                Collection<CountersAndTimers> cats = a.getAllTenantSpecificMetrics();
                for (CountersAndTimers c : cats) {
                    String tenant = c.getName().split(">")[2];
                    gather(tenant, c, metrics, time);
                }
            }
        }
        if (jvmMetricsEnabled) {
            metrics.addAll(jvmMetrics.sample(datacenter, cluster, host, service, instance, version));
        }
        return metrics;
    }

    private void gather(String tenant, CountersAndTimers a, List<AnomalyMetric> metrics, String time) {
        for (Entry<String, Counter> counter : a.getCounters()) {
            metrics.add(new AnomalyMetric(
                datacenter,
                cluster,
                host,
                service,
                instance,
                version,
                tenant,
                a.getName(),
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
                tenant,
                a.getName(),
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
                tenant,
                a.getName(),
                timers.getKey().split("\\>"),
                "timer",
                timers.getValue().getLastSample(),
                time));
        }
    }

}
