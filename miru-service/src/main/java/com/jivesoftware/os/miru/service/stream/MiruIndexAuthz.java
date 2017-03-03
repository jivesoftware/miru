package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 *
 */
public class MiruIndexAuthz<BM extends IBM, IBM> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    public List<Future<?>> index(final MiruContext<BM, IBM, ?> context,
        MiruTenantId tenantId,
        final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds,
        ExecutorService indexExecutor)
        throws Exception {

        //TODO create work based on distinct authz strings
        return Collections.<Future<?>>singletonList(indexExecutor.submit(() -> {
            StackBuffer stackBuffer = new StackBuffer();
            for (MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
                MiruInternalActivity activity = internalActivityAndId.activity;
                if (activity.authz != null) {
                    log.inc("count>set", activity.authz.length);
                    log.inc("count>set", activity.authz.length, tenantId.toString());
                    for (String authz : activity.authz) {
                        context.authzIndex.set(authz, stackBuffer, internalActivityAndId.id);
                    }
                }
            }
            return null;
        }));
    }

}
