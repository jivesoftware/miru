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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSampleEvent;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Objects.firstNonNull;

/**
 * @author jonathan.colt
 */
public class SampleMill {

    private final OrderIdProvider idProvider;

    public final Table<ServiceId, String, AtomicLong> levelCounts = HashBasedTable.create();

    public SampleMill(OrderIdProvider idProvider) {
        this.idProvider = idProvider;
    }

    MiruActivity mill(MiruTenantId tenantId, MiruMetricSampleEvent event) {
        ServiceId serviceId = new ServiceId(
            firstNonNull(event.datacenter, "unknown"),
            firstNonNull(event.cluster, "unknown"),
            firstNonNull(event.host, "unknown"),
            firstNonNull(event.service, "unknown"),
            firstNonNull(event.instance, "unknown"),
            firstNonNull(event.version, "unknown"));

        AtomicLong levelCount = levelCounts.get(serviceId, "Sample");
        if (levelCount == null) {
            levelCount = new AtomicLong();
            levelCounts.put(serviceId, "Sample", levelCount);
        }
        levelCount.incrementAndGet();

        return new MiruActivity.Builder(tenantId, idProvider.nextId(), new String[0], 0)
            .putFieldValue("datacenter", firstNonNull(event.datacenter, "unknown"))
            .putFieldValue("cluster", firstNonNull(event.cluster, "unknown"))
            .putFieldValue("host", firstNonNull(event.host, "unknown"))
            .putFieldValue("service", firstNonNull(event.service, "unknown"))
            .putFieldValue("instance", firstNonNull(event.instance, "unknown"))
            .putFieldValue("version", firstNonNull(event.version, "unknown"))
            //.putAllFieldValues("metrics", tokenize(event.message))
            .putFieldValue("timestamp", firstNonNull(event.timestamp, "unknown"))
            .build();
    }

}
