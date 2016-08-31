package com.jivesoftware.os.miru.stumptown.deployable;

import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.logappender.MiruLogEvent;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Objects.firstNonNull;

/**
 * @author jonathan.colt
 */
public class LogMill {

    private final OrderIdProvider idProvider;

    public final Table<ServiceId, String, AtomicLong> levelCounts = HashBasedTable.create();

    public LogMill(OrderIdProvider idProvider) {
        this.idProvider = idProvider;
    }

    MiruActivity mill(MiruTenantId tenantId, MiruLogEvent logEvent) {
        ServiceId serviceId = new ServiceId(
            firstNonNull(Strings.emptyToNull(logEvent.datacenter), "unknown"),
            firstNonNull(Strings.emptyToNull(logEvent.cluster), "unknown"),
            firstNonNull(Strings.emptyToNull(logEvent.host), "unknown"),
            firstNonNull(Strings.emptyToNull(logEvent.service), "unknown"),
            firstNonNull(Strings.emptyToNull(logEvent.instance), "unknown"),
            firstNonNull(Strings.emptyToNull(logEvent.version), "unknown"));

        AtomicLong levelCount = levelCounts.get(serviceId, logEvent.level);
        if (levelCount == null) {
            levelCount = new AtomicLong();
            levelCounts.put(serviceId, logEvent.level, levelCount);
        }
        levelCount.incrementAndGet();

        return new MiruActivity.Builder(tenantId, idProvider.nextId(), 0, false, new String[0])
            .putFieldValue("datacenter", firstNonNull(Strings.emptyToNull(logEvent.datacenter), "unknown"))
            .putFieldValue("cluster", firstNonNull(Strings.emptyToNull(logEvent.cluster), "unknown"))
            .putFieldValue("host", firstNonNull(Strings.emptyToNull(logEvent.host), "unknown"))
            .putFieldValue("service", firstNonNull(Strings.emptyToNull(logEvent.service), "unknown"))
            .putFieldValue("instance", firstNonNull(Strings.emptyToNull(logEvent.instance), "unknown"))
            .putFieldValue("version", firstNonNull(Strings.emptyToNull(logEvent.version), "unknown"))
            .putFieldValue("level", firstNonNull(Strings.emptyToNull(logEvent.level), "unknown"))
            .putFieldValue("thread", firstNonNull(Strings.emptyToNull(logEvent.threadName), "unknown"))
            .putFieldValue("methodName", firstNonNull(Strings.emptyToNull(logEvent.methodName), "unknown"))
            .putFieldValue("lineNumber", firstNonNull(Strings.emptyToNull(logEvent.lineNumber), "unknown"))
            .putFieldValue("logger", firstNonNull(Strings.emptyToNull(logEvent.loggerName), "unknown"))
            .putAllFieldValues("message", tokenize(logEvent.message))
            .putFieldValue("timestamp", firstNonNull(Strings.emptyToNull(logEvent.timestamp), "unknown"))
            .putFieldValue("exceptionClass", firstNonNull(Strings.emptyToNull(logEvent.exceptionClass), "unknown"))
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
        String[] split = raw.toLowerCase().split("[^a-zA-Z0-9']+");
        HashSet<String> set = Sets.newHashSet();
        for (String s : split) {
            if (!Strings.isNullOrEmpty(s)) {
                set.add(s);
            }
        }
        return set;
    }
}
