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
package com.jivesoftware.os.miru.lumberyard.deployable;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.logappender.MiruLogEvent;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Objects.firstNonNull;

/**
 * @author jonathan.colt
 */
public class LogMill {

    private final OrderIdProvider idProvider;

    private final Table<ServiceId, String, AtomicLong> levelCounts = HashBasedTable.create();

    public LogMill(OrderIdProvider idProvider) {
        this.idProvider = idProvider;
    }

    MiruActivity mill(MiruTenantId tenantId, MiruLogEvent logEvent) {
        ServiceId serviceId = new ServiceId(
            firstNonNull(logEvent.datacenter, "unknown"),
            firstNonNull(logEvent.cluster, "unknown"),
            firstNonNull(logEvent.host, "unknown"),
            firstNonNull(logEvent.service, "unknown"),
            firstNonNull(logEvent.instance, "unknown"),
            firstNonNull(logEvent.version, "unknown"));

        AtomicLong levelCount = levelCounts.get(serviceId, logEvent.level);
        if (levelCount == null) {
            levelCount = new AtomicLong();
            levelCounts.put(serviceId, logEvent.level, levelCount);
        }
        levelCount.incrementAndGet();

        return new MiruActivity.Builder(tenantId, idProvider.nextId(), new String[0], 0)
            .putFieldValue("datacenter", firstNonNull(logEvent.datacenter, "unknown"))
            .putFieldValue("cluster", firstNonNull(logEvent.cluster, "unknown"))
            .putFieldValue("host", firstNonNull(logEvent.host, "unknown"))
            .putFieldValue("service", firstNonNull(logEvent.service, "unknown"))
            .putFieldValue("instance", firstNonNull(logEvent.instance, "unknown"))
            .putFieldValue("version", firstNonNull(logEvent.version, "unknown"))
            .putFieldValue("level", firstNonNull(logEvent.level, "unknown"))
            .putFieldValue("thread", firstNonNull(logEvent.threadName, "unknown"))
            .putFieldValue("logger", firstNonNull(logEvent.loggerName, "unknown"))
            .putAllFieldValues("message", tokenize(logEvent.message))
            .putFieldValue("timestamp", firstNonNull(logEvent.timestamp, "unknown"))
            .putAllFieldValues("thrownStackTrace", tokenizeStackTrace(logEvent.thrownStackTrace))
            .build();
    }

    private Set<String> tokenizeStackTrace(String[] thrownStackTrace) {
        Set<String> tokenized = Sets.newHashSet();
        if (thrownStackTrace != null) {
            for (String stackTrace : thrownStackTrace) {
                tokenized.addAll(tokenize(stackTrace));
            }
        }
        return tokenized;
    }

    private Set<String> tokenize(String raw) {
        if (raw == null) {
            return Collections.emptySet();
        }
        return Sets.newHashSet(raw.toLowerCase().split("\\s+"));
    }
}
