package com.jivesoftware.os.miru.service.stream;

import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 *
 */
public class MiruIndexAuthz<BM> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    public List<Future<?>> index(final MiruContext<BM, ?> context,
        MiruTenantId tenantId,
        final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds,
        final boolean repair,
        ExecutorService indexExecutor)
        throws Exception {

        //TODO create work based on distinct authz strings
        return Collections.<Future<?>>singletonList(indexExecutor.submit(() -> {
            StackBuffer stackBuffer = new StackBuffer();
            for (MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
                MiruInternalActivity activity = internalActivityAndId.activity;
                if (activity.authz != null) {
                    if (repair) {
                        log.inc("count>set", activity.authz.length);
                        log.inc("count>set", activity.authz.length, tenantId.toString());
                        for (String authz : activity.authz) {
                            context.authzIndex.set(authz, stackBuffer, internalActivityAndId.id);
                        }
                    } else {
                        log.inc("count>append", activity.authz.length);
                        log.inc("count>append", activity.authz.length, tenantId.toString());
                        for (String authz : activity.authz) {
                            context.authzIndex.append(authz, stackBuffer, internalActivityAndId.id);
                        }
                    }
                }
            }
            return null;
        }));
    }

    //TODO move this behavior into index()
    private List<Future<?>> repair(final MiruContext<BM, ?> context,
        final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds,
        final List<MiruInternalActivity> existingActivities,
        ExecutorService indexExecutor)
        throws Exception {

        return Collections.<Future<?>>singletonList(indexExecutor.submit(() -> {
            StackBuffer stackBuffer = new StackBuffer();
            for (int i = 0; i < internalActivityAndIds.size(); i++) {
                MiruActivityAndId<MiruInternalActivity> internalActivityAndId = internalActivityAndIds.get(i);
                MiruInternalActivity internalActivity = internalActivityAndId.activity;
                int id = internalActivityAndId.id;
                MiruInternalActivity existing = existingActivities.get(i);

                Set<String> existingAuthz = existing != null && existing.authz != null ? Sets.newHashSet(existing.authz) : Sets.<String>newHashSet();
                Set<String> repairedAuthz = internalActivity.authz != null ? Sets.newHashSet(internalActivity.authz) : Sets.<String>newHashSet();

                for (String authz : existingAuthz) {
                    if (!repairedAuthz.contains(authz)) {
                        context.authzIndex.remove(authz, id, stackBuffer);
                    }
                }

                for (String authz : repairedAuthz) {
                    if (!existingAuthz.contains(authz)) {
                        context.authzIndex.set(authz, stackBuffer, id);
                    }
                }
            }
            return null;
        }));
    }

}
