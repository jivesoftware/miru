package com.jivesoftware.os.miru.metric.sampler;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

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
    public Map<MetricKey, Long> metricAndValue;
    public String timestamp;

    public MiruMetricSampleEvent() {
    }

    public MiruMetricSampleEvent(String datacenter,
        String cluster,
        String host,
        String service,
        String instance,
        String version,
        Map<MetricKey, Long> metricAndValue,
        String timestamp) {
        this.datacenter = datacenter;
        this.cluster = cluster;
        this.host = host;
        this.service = service;
        this.instance = instance;
        this.version = version;
        this.metricAndValue = metricAndValue;
        this.timestamp = timestamp;
    }

    public static class MetricKey {

        public String sampler;
        public String[] path;
        public String type;

        public MetricKey() {
        }

        public MetricKey(String sampler, String[] path, String type) {
            this.sampler = sampler;
            this.path = path;
            this.type = type;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 97 * hash + Objects.hashCode(this.sampler);
            hash = 97 * hash + Arrays.deepHashCode(this.path);
            hash = 97 * hash + Objects.hashCode(this.type);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final MetricKey other = (MetricKey) obj;
            if (!Objects.equals(this.sampler, other.sampler)) {
                return false;
            }
            if (!Arrays.deepEquals(this.path, other.path)) {
                return false;
            }
            if (!Objects.equals(this.type, other.type)) {
                return false;
            }
            return true;
        }


    }
}
