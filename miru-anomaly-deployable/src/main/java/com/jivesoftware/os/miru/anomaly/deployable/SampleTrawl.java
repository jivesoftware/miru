package com.jivesoftware.os.miru.anomaly.deployable;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.metric.sampler.AnomalyMetric;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Objects.firstNonNull;

/**
 * @author jonathan.colt
 */
public class SampleTrawl {

    private final OrderIdProvider idProvider;

    public final Table<ServiceId, String, AtomicLong> trawled = HashBasedTable.create();

    public SampleTrawl(OrderIdProvider idProvider) {
        this.idProvider = idProvider;
    }

    MiruActivity trawl(MiruTenantId tenantId, AnomalyMetric metric) {

        ServiceId serviceId = new ServiceId(
            firstNonNull(Strings.emptyToNull(metric.datacenter), "unknown"),
            firstNonNull(Strings.emptyToNull(metric.cluster), "unknown"),
            firstNonNull(Strings.emptyToNull(metric.host), "unknown"),
            firstNonNull(Strings.emptyToNull(metric.service), "unknown"),
            firstNonNull(Strings.emptyToNull(metric.instance), "unknown"),
            firstNonNull(Strings.emptyToNull(metric.version), "unknown"));

        AtomicLong levelCount = trawled.get(serviceId, "ingressed");
        if (levelCount == null) {
            levelCount = new AtomicLong();
            trawled.put(serviceId, "ingressed", levelCount);
        }
        levelCount.incrementAndGet();

        long value = metric.value;

        List<String> bits = new ArrayList<>();
        if (value >= 0) {
            bits.add("+");
        } else {
            bits.add("-");
            if (value == Long.MIN_VALUE) {
                value = Long.MAX_VALUE; // lossy, but sign flip would remain negative
            } else {
                value = -value;
            }
        }

        String metricName = Joiner.on(">").join(metric.path);
        for (int i = 0; i < 64; i++) {
            if (((value >>> i) & 1) != 0) {
                bits.add(String.valueOf(i));
            }
        }

        return new MiruActivity.Builder(tenantId, idProvider.nextId(), 0, false, new String[0])
            .putFieldValue("datacenter", firstNonNull(Strings.emptyToNull(metric.datacenter), "unknown"))
            .putFieldValue("cluster", firstNonNull(Strings.emptyToNull(metric.cluster), "unknown"))
            .putFieldValue("host", firstNonNull(Strings.emptyToNull(metric.host), "unknown"))
            .putFieldValue("service", firstNonNull(Strings.emptyToNull(metric.service), "unknown"))
            .putFieldValue("instance", firstNonNull(Strings.emptyToNull(metric.instance), "unknown"))
            .putFieldValue("version", firstNonNull(Strings.emptyToNull(metric.version), "unknown"))
            .putFieldValue("sampler", firstNonNull(Strings.emptyToNull(metric.sampler), "unknown"))
            .putFieldValue("metric", firstNonNull(Strings.emptyToNull(metricName), "unknown"))
            .putAllFieldValues("bits", bits)
            .putAllFieldValues("tags", sanitize(metric.path))
            .putFieldValue("type", firstNonNull(Strings.emptyToNull(metric.type), "unknown"))
            .putFieldValue("tenant", firstNonNull(Strings.emptyToNull(metric.tenant), "unknown"))
            .putFieldValue("timestamp", firstNonNull(Strings.emptyToNull(metric.timestamp), "unknown"))
            .build();
    }

    List<String> sanitize(String[] path) {
        List<String> sanitized = new ArrayList<>();
        for (String p : path) {
            if (!Strings.isNullOrEmpty(p)) {
                sanitized.add(p);
            }
        }
        return sanitized;
    }

    public static void main(String[] args) {
        long expected = 0;
        int[] bits = new int[64];

        for (int a = 0; a < 10; a++) {
            long o = new Random().nextLong();
            expected += o;
            long l = o;

            /*
             int i = 0;
             while (l != 0L) {
             if (l % 2L != 0) {
             bits[i]++;
             }
             ++i;
             l = l >>> 1;
             }*/
            for (int i = 0; i < 64; i++) {
                if (((l >> i) & 1) != 0) {
                    bits[i]++;
                }
            }

        }

        long r = 0;
        for (int i = 0; i < 64; i++) {
            if (bits[i] > 0) {
                r += (bits[i] * (1L << i));
            }
        }

        System.out.println(expected + " " + r);
    }

}
