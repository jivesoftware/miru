package com.jivesoftware.os.miru.service.stream;

import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 *
 */
public class MiruIndexAuthz<BM> {

    public List<Future<?>> index(final MiruContext<BM, ?> context,
        final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds,
        final boolean repair,
        ExecutorService indexExecutor)
        throws Exception {

        //TODO create work based on distinct authz strings
        return Arrays.<Future<?>>asList(indexExecutor.submit(() -> {
            for (MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
                MiruInternalActivity activity = internalActivityAndId.activity;
                if (activity.authz != null) {
                    for (String authz : activity.authz) {
                        if (repair) {
                            context.authzIndex.set(authz, internalActivityAndId.id);
                        } else {
                            context.authzIndex.append(authz, internalActivityAndId.id);
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

        return Arrays.<Future<?>>asList(indexExecutor.submit(() -> {
            for (int i = 0; i < internalActivityAndIds.size(); i++) {
                MiruActivityAndId<MiruInternalActivity> internalActivityAndId = internalActivityAndIds.get(i);
                MiruInternalActivity internalActivity = internalActivityAndId.activity;
                int id = internalActivityAndId.id;
                MiruInternalActivity existing = existingActivities.get(i);

                Set<String> existingAuthz = existing != null && existing.authz != null ? Sets.newHashSet(existing.authz) : Sets.<String>newHashSet();
                Set<String> repairedAuthz = internalActivity.authz != null ? Sets.newHashSet(internalActivity.authz) : Sets.<String>newHashSet();

                for (String authz : existingAuthz) {
                    if (!repairedAuthz.contains(authz)) {
                        context.authzIndex.remove(authz, id);
                    }
                }

                for (String authz : repairedAuthz) {
                    if (!existingAuthz.contains(authz)) {
                        context.authzIndex.set(authz, id);
                    }
                }
            }
            return null;
        }));
    }

}
