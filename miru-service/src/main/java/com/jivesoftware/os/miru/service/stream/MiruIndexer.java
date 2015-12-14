package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
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
    private final MiruIndexFieldValues<BM, IBM> indexFieldValues;
    private final MiruIndexBloom<BM, IBM> indexBloom;
    private final MiruIndexLatest<BM, IBM> indexLatest;
    private final MiruIndexPairedLatest<BM, IBM> indexPairedLatest;

    public MiruIndexer(MiruIndexAuthz<BM, IBM> indexAuthz,
        MiruIndexFieldValues<BM, IBM> indexFieldValues,
        MiruIndexBloom<BM, IBM> indexBloom,
        MiruIndexLatest<BM, IBM> indexLatest,
        MiruIndexPairedLatest<BM, IBM> indexPairedLatest) {
        this.indexAuthz = indexAuthz;
        this.indexFieldValues = indexFieldValues;
        this.indexBloom = indexBloom;
        this.indexLatest = indexLatest;
        this.indexPairedLatest = indexPairedLatest;
    }

    public void index(final MiruContext<BM, IBM, ?> context,
        final MiruPartitionCoord coord,
        final List<MiruActivityAndId<MiruActivity>> activityAndIds,
        boolean repair,
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
        List<Future<List<FieldValuesWork>>> fieldValuesComposed = indexFieldValues.compose(context, internalActivityAndIds, indexExecutor);
        List<Future<List<BloomWork>>> bloomComposed = indexBloom.compose(context, internalActivityAndIds, indexExecutor);
        List<Future<List<PairedLatestWork>>> pairedLatestComposed = indexPairedLatest.compose(context, internalActivityAndIds, indexExecutor);

        // 2. Prepare work
        Future<List<BloomWork>> bloomPrepared = indexBloom.prepare(context, bloomComposed, indexExecutor);
        Future<List<PairedLatestWork>> pairedLatestPrepared = indexPairedLatest.prepare(context, pairedLatestComposed, indexExecutor);

        // 3. Index field values work
        List<Future<?>> fieldFutures = indexFieldValues.index(context, coord.tenantId, fieldValuesComposed, repair, indexExecutor);

        // 4. Wait for completion
        awaitFutures(fieldFutures, "indexFieldValues");

        // 5. Index remaining work
        final List<Future<?>> otherFutures = new ArrayList<>();
        otherFutures.addAll(indexAuthz.index(context, coord.tenantId, internalActivityAndIds, repair, indexExecutor));
        otherFutures.addAll(indexBloom.index(context, coord.tenantId, bloomPrepared, repair, indexExecutor));
        otherFutures.addAll(indexLatest.index(context, coord.tenantId, internalActivityAndIds, repair, indexExecutor));
        otherFutures.addAll(indexPairedLatest.index(context, coord.tenantId, pairedLatestPrepared, repair, indexExecutor));

        // 6. Update activity index
        otherFutures.add(indexExecutor.submit(() -> {
            StackBuffer stackBuffer = new StackBuffer();
            context.activityIndex.set(internalActivityAndIds, stackBuffer);
            return null;
        }));

        // 7. Update removal index
        if (repair) {
            otherFutures.add(indexExecutor.submit(() -> {
                // repairs also unhide (remove from removal)
                log.inc("count>remove", activityAndIds.size());
                log.inc("count>remove", activityAndIds.size(), coord.tenantId.toString());
                StackBuffer stackBuffer = new StackBuffer();
                for (MiruActivityAndId<MiruActivity> activityAndId : activityAndIds) {
                    context.removalIndex.remove(activityAndId.id, stackBuffer);
                }
                return null;
            }));
        }

        // 8. Wait for completion
        awaitFutures(otherFutures, "indexOther");

        // 9. Mark as ready
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
        context.activityIndex.setAndReady(internalActivityAndIds, stackBuffer);
    }

    /*public void repair(final MiruContext<BM> context,
        final List<MiruActivityAndId<MiruActivity>> activityAndIds,
        ExecutorService indexExecutor)
        throws Exception {

        @SuppressWarnings("unchecked")
        List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds = Arrays.<MiruActivityAndId<MiruInternalActivity>>asList(
            new MiruActivityAndId[activityAndIds.size()]);
        List<MiruInternalActivity> existingActivities = Lists.newArrayListWithCapacity(activityAndIds.size());
        for (MiruActivityAndId<MiruActivity> activityAndId : activityAndIds) {
            existingActivities.add(context.activityIndex.get(activityAndId.activity.tenantId, activityAndId.id));
        }
        context.activityInternExtern.intern(activityAndIds, 0, activityAndIds.size(), internalActivityAndIds, context.schema);

        awaitFutures(indexFieldValues.repair(context, internalActivityAndIds, existingActivities, indexExecutor), "repairFieldValues");

        List<Future<?>> otherFutures = Lists.newArrayList();
        otherFutures.addAll(indexAuthz.repair(context, internalActivityAndIds, existingActivities, indexExecutor));
        otherFutures.addAll(indexBloom.repair(context, internalActivityAndIds, existingActivities, indexExecutor));
        otherFutures.addAll(indexLatest.repair(context, internalActivityAndIds, existingActivities, indexExecutor));
        otherFutures.addAll(indexPairedLatest.repair(context, internalActivityAndIds, existingActivities, indexExecutor));

        otherFutures.add(indexExecutor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                // repairs also unhide (remove from removal)
                for (MiruActivityAndId<MiruActivity> activityAndId : activityAndIds) {
                    context.removalIndex.remove(activityAndId.id);
                }
                return null;
            }
        }));

        awaitFutures(otherFutures, "repairOther");

        // finally, update the activity index
        context.activityIndex.setAndReady(internalActivityAndIds);
    }*/

    public void remove(MiruContext<BM, IBM, ?> context, MiruActivity activity, int id) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruInternalActivity existing = context.activityIndex.get(activity.tenantId, id, stackBuffer);
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
            context.activityIndex.setAndReady(internalActivity, stackBuffer);
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
