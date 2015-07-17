package com.jivesoftware.os.miru.metric.sampler;

import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public interface MiruMetricSampleSender {
    void send(List<AnomalyMetric> events) throws Exception;
}
