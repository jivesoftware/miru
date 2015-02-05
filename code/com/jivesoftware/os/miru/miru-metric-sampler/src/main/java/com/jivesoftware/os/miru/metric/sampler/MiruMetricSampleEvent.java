package com.jivesoftware.os.miru.metric.sampler;

import java.util.List;

/**
 *
 */
public class MiruMetricSampleEvent {

    public String datacenter;
    public String cluster;
    public String host;
    public String service;
    public String instance;
    public String version;
    public List<Metric> metrics;
    public String timestamp;

    public MiruMetricSampleEvent() {
    }

    public MiruMetricSampleEvent(String datacenter,
        String cluster,
        String host,
        String service,
        String instance,
        String version,
        List<Metric> metrics,
        String timestamp) {
        this.datacenter = datacenter;
        this.cluster = cluster;
        this.host = host;
        this.service = service;
        this.instance = instance;
        this.version = version;
        this.metrics = metrics;
        this.timestamp = timestamp;
    }

}
