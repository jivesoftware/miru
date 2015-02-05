package com.jivesoftware.os.miru.metric.sampler;

/**
 *
 */
public class AnomalyMetric {

    public String datacenter;
    public String cluster;
    public String host;
    public String service;
    public String instance;
    public String version;
    public String sampler;
    public String[] path;
    public String type;
    public long value;
    public String timestamp;

    public AnomalyMetric() {
    }

    public AnomalyMetric(String datacenter,
        String cluster,
        String host,
        String service,
        String instance,
        String version,
        String sampler,
        String[] path,
        String type,
        long value,
        String timestamp) {
        this.datacenter = datacenter;
        this.cluster = cluster;
        this.host = host;
        this.service = service;
        this.instance = instance;
        this.version = version;
        this.sampler = sampler;
        this.path = path;
        this.type = type;
        this.value = value;
        this.timestamp = timestamp;
    }

}
