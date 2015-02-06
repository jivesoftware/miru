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
import com.jivesoftware.os.miru.metric.sampler.AnomalyMetric;
import java.util.ArrayList;
import java.util.Arrays;
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

    MiruActivity trawl(MiruTenantId tenantId, AnomalyMetric metric) {

        ServiceId serviceId = new ServiceId(
            firstNonNull(metric.datacenter, "unknown"),
            firstNonNull(metric.cluster, "unknown"),
            firstNonNull(metric.host, "unknown"),
            firstNonNull(metric.service, "unknown"),
            firstNonNull(metric.instance, "unknown"),
            firstNonNull(metric.version, "unknown"));

        AtomicLong levelCount = trawled.get(serviceId, "ingressed");
        if (levelCount == null) {
            levelCount = new AtomicLong();
            trawled.put(serviceId, "ingressed", levelCount);
        }
        levelCount.incrementAndGet();

        List<String> bits = new ArrayList<>();

        String metricName = Joiner.on(">").join(metric.path);
        for (int i = 0; i < 64; i++) {
            if (((metric.value >> i) & 1) != 0) {
                bits.add(String.valueOf(i));
            }
        }

        return new MiruActivity.Builder(tenantId, idProvider.nextId(), new String[0], 0)
            .putFieldValue("datacenter", firstNonNull(metric.datacenter, "unknown"))
            .putFieldValue("cluster", firstNonNull(metric.cluster, "unknown"))
            .putFieldValue("host", firstNonNull(metric.host, "unknown"))
            .putFieldValue("service", firstNonNull(metric.service, "unknown"))
            .putFieldValue("instance", firstNonNull(metric.instance, "unknown"))
            .putFieldValue("version", firstNonNull(metric.version, "unknown"))
            .putFieldValue("sampler", metric.sampler)
            .putFieldValue("metric", metricName)
            .putAllFieldValues("bits", bits)
            .putAllFieldValues("tags", Arrays.asList(metric.path))
            .putFieldValue("type", metric.type)
            .putFieldValue("timestamp", firstNonNull(metric.timestamp, "unknown"))
            .build();
    }

}
