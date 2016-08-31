package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.plugin.index.TimeVersionRealtime;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Handles indexing of activity, including repair and removal, with synchronization and attention to versioning.
 */
public class MiruIndexer<BM extends IBM, IBM> {

    private final static MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruIndexAuthz<BM, IBM> indexAuthz;
    private final MiruIndexPrimaryFields<BM, IBM> indexPrimaryFields;
    private final MiruIndexValueBits<BM, IBM> indexValueBits;
    private final MiruIndexBloom<BM, IBM> indexBloom;
    private final MiruIndexLatest<BM, IBM> indexLatest;
    private final MiruIndexPairedLatest<BM, IBM> indexPairedLatest;

    public MiruIndexer(MiruIndexAuthz<BM, IBM> indexAuthz,
        MiruIndexPrimaryFields<BM, IBM> indexPrimaryFields,
        MiruIndexValueBits<BM, IBM> indexValueBits,
        MiruIndexBloom<BM, IBM> indexBloom,
        MiruIndexLatest<BM, IBM> indexLatest,
        MiruIndexPairedLatest<BM, IBM> indexPairedLatest) {
        this.indexAuthz = indexAuthz;
        this.indexPrimaryFields = indexPrimaryFields;
        this.indexValueBits = indexValueBits;
        this.indexBloom = indexBloom;
        this.indexLatest = indexLatest;
        this.indexPairedLatest = indexPairedLatest;
    }

    public void index(final MiruContext<BM, IBM, ?> context,
        final MiruPartitionCoord coord,
        final List<MiruActivityAndId<MiruActivity>> activityAndIds,
        ExecutorService indexExecutor)
        throws Exception {

        if (activityAndIds.isEmpty()) {
            return;
        }

        @SuppressWarnings("unchecked")
        final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds = Arrays.<MiruActivityAndId<MiruInternalActivity>>asList(
            new MiruActivityAndId[activityAndIds.size()]);

        log.debug("Start: Index batch of {}", activityAndIds.size());

        final int numPartitions = 48;
        final int numActivities = internalActivityAndIds.size();
        final int partitionSize = (numActivities + numPartitions - 1) / numPartitions;

        List<Future<?>> internFutures = new ArrayList<>(numPartitions);
        for (int i = 0; i < activityAndIds.size(); i += partitionSize) {
            final int startOfSubList = i;
            internFutures.add(indexExecutor.submit(() -> {
                StackBuffer stackBuffer = new StackBuffer();
                context.activityInternExtern.intern(activityAndIds, startOfSubList, partitionSize, internalActivityAndIds, context.schema, stackBuffer);
                return null;
            }));
        }
        awaitFutures(internFutures, "indexIntern");

        // free for GC before we begin indexing
        activityAndIds.clear();

        // 1. Compose work
        List<Future<List<PrimaryIndexWork>>> primaryFieldsComposed = indexPrimaryFields.compose(context, internalActivityAndIds, indexExecutor);

        // 2. Index field values work
        List<Future<?>> primaryFieldFutures = indexPrimaryFields.index(context, coord.tenantId, primaryFieldsComposed, indexExecutor);

        // 3. Wait for completion
        awaitFutures(primaryFieldFutures, "indexPrimaryFields");

        // 4. Index remaining work
        final List<Future<?>> otherFutures = new ArrayList<>();
        otherFutures.addAll(indexAuthz.index(context, coord.tenantId, internalActivityAndIds, indexExecutor));
        otherFutures.addAll(indexLatest.index(context, coord.tenantId, internalActivityAndIds, indexExecutor));

        // 5. Update activity index
        otherFutures.add(indexExecutor.submit(() -> {
            StackBuffer stackBuffer = new StackBuffer();
            context.activityIndex.set(context.schema, internalActivityAndIds, stackBuffer);
            return null;
        }));

        /*TODO really? reevaluate if we need removes
        // 6. Update removal index
        otherFutures.add(indexExecutor.submit(() -> {
            // repairs also unhide (remove from removal)
            log.inc("count>remove", activityAndIds.size());
            log.inc("count>remove", activityAndIds.size(), coord.tenantId.toString());
            StackBuffer stackBuffer = new StackBuffer();
            TIntList ids = new TIntArrayList();
            for (MiruActivityAndId<MiruActivity> activityAndId : activityAndIds) {
                ids.add(activityAndId.id);
            }
            context.removalIndex.remove(stackBuffer, ids.toArray());
            return null;
        }));
        */

        // 7. Wait for completion
        awaitFutures(otherFutures, "indexOther");

        // 8. Mark as ready
        StackBuffer stackBuffer = new StackBuffer();
        context.activityIndex.ready(internalActivityAndIds.get(internalActivityAndIds.size() - 1).id, stackBuffer);

        log.debug("End: Index batch of {}", internalActivityAndIds.size());
    }

    public void set(MiruContext<BM, IBM, ?> context, List<MiruActivityAndId<MiruActivity>> activityAndIds) throws Exception {
        @SuppressWarnings("unchecked")
        List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds = Arrays.<MiruActivityAndId<MiruInternalActivity>>asList(
            new MiruActivityAndId[activityAndIds.size()]);

        StackBuffer stackBuffer = new StackBuffer();
        context.activityInternExtern.intern(activityAndIds, 0, activityAndIds.size(), internalActivityAndIds, context.schema, stackBuffer);
        context.activityIndex.setAndReady(context.schema, internalActivityAndIds, stackBuffer);
    }

    public void remove(MiruContext<BM, IBM, ?> context, MiruActivity activity, int id) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        TimeVersionRealtime existing = context.activityIndex.getTimeVersionRealtime("remove", id, stackBuffer);
        if (existing == null) {
            log.debug("Can't remove nonexistent activity at {}\n- offered: {}", id, activity);
        } else if (activity.version <= existing.version) {
            log.debug("Declined to remove old activity at {}\n- have: {}\n- offered: {}", id, existing, activity);
        } else {
            log.debug("Removing activity at {}\n- was: {}\n- now: {}", id, existing, activity);
            @SuppressWarnings("unchecked")
            List<MiruActivityAndId<MiruInternalActivity>> internalActivity = Arrays.<MiruActivityAndId<MiruInternalActivity>>asList(
                new MiruActivityAndId[1]);
            context.activityInternExtern.intern(Collections.singletonList(new MiruActivityAndId<>(activity, id)), 0, 1, internalActivity, context.schema,
                stackBuffer);

            //TODO apply field changes?
            // hide (add to removal)
            context.removalIndex.set(stackBuffer, id);

            // finally, update the activity index
            context.activityIndex.setAndReady(context.schema, internalActivity, stackBuffer);
        }
    }

    private void awaitFutures(List<Future<?>> futures, String futureName) throws InterruptedException, ExecutionException {
        long start = System.currentTimeMillis();
        for (Future<?> future : futures) {
            future.get();
        }
        if (log.isTraceEnabled()) {
            log.trace(futureName + ": Finished waiting for futures in " + (System.currentTimeMillis() - start) + " ms");
        }
    }
}
