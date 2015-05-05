package com.jivesoftware.os.miru.metric.sampler;

/**
 *
 * @author jonathan.colt
 */
public interface MiruMetricSampler {

    void start();

    void stop();

    boolean isStarted();
}
