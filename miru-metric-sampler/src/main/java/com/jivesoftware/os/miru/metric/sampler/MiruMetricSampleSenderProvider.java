package com.jivesoftware.os.miru.metric.sampler;

/**
 *
 * @author jonathan.colt
 */
public interface MiruMetricSampleSenderProvider {

    MiruMetricSampleSender[] getSenders();
}
