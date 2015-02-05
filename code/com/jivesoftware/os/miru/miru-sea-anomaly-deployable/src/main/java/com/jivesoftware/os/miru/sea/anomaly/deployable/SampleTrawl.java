/*
 * Copyright 2015 jonathan.colt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.miru.sea.anomaly.deployable;

import com.google.common.base.Joiner;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.metric.sampler.Metric;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSampleEvent;
import java.util.ArrayList;
import java.util.List;
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

    MiruActivity trawl(MiruTenantId tenantId, MiruMetricSampleEvent event) {
        if (event.metrics.isEmpty()) {
            return null;
        }

        ServiceId serviceId = new ServiceId(
            firstNonNull(event.datacenter, "unknown"),
            firstNonNull(event.cluster, "unknown"),
            firstNonNull(event.host, "unknown"),
            firstNonNull(event.service, "unknown"),
            firstNonNull(event.instance, "unknown"),
            firstNonNull(event.version, "unknown"));

        AtomicLong levelCount = trawled.get(serviceId, "sample");
        if (levelCount == null) {
            levelCount = new AtomicLong();
            trawled.put(serviceId, "sample", levelCount);
        }
        levelCount.incrementAndGet();

        List<String> metrics = new ArrayList<>();
        List<String> bits = new ArrayList<>();
        List<String> samplers = new ArrayList<>();
        List<String> tags = new ArrayList<>();
        List<String> types = new ArrayList<>();
        for (Metric metric : event.metrics) {
            String metricName = metric.sampler + " " + Joiner.on(">").join(metric.path) + " " + metric.type;
            metrics.add(metricName);
            for (int i = 0; i < 64; i++) {
                if (((metric.value >> i) & 1) != 0) {
                    bits.add(metricName + "-" + i);
                }
            }

            for (String tag : metric.path) {
                tags.add(tag + ":" + metricName);
            }

            samplers.add(metric.sampler + ":" + metricName);
            types.add(metric.type + ":" + metricName);

        }

        return new MiruActivity.Builder(tenantId, idProvider.nextId(), new String[0], 0)
            .putFieldValue("datacenter", firstNonNull(event.datacenter, "unknown"))
            .putFieldValue("cluster", firstNonNull(event.cluster, "unknown"))
            .putFieldValue("host", firstNonNull(event.host, "unknown"))
            .putFieldValue("service", firstNonNull(event.service, "unknown"))
            .putFieldValue("instance", firstNonNull(event.instance, "unknown"))
            .putFieldValue("version", firstNonNull(event.version, "unknown"))
            .putAllFieldValues("samplers", samplers)
            .putAllFieldValues("metrics", metrics)
            .putAllFieldValues("bits", bits)
            .putAllFieldValues("tags", tags)
            .putAllFieldValues("types", types)
            .putFieldValue("timestamp", firstNonNull(event.timestamp, "unknown"))
            .build();
    }

}
