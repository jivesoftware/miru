package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Handles indexing of activity, including repair and removal, with synchronization and attention to versioning.
 */
public class MiruIndexer<BM> {

    private final static MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruIndexAuthz<BM> indexAuthz;
    private final MiruIndexFieldValues<BM> indexFieldValues;
    private final MiruIndexBloom<BM> indexBloom;
    private final MiruIndexLatest<BM> indexLatest;
    private final MiruIndexPairedLatest<BM> indexPairedLatest;

    public MiruIndexer(MiruIndexAuthz<BM> indexAuthz,
        MiruIndexFieldValues<BM> indexFieldValues,
        MiruIndexBloom<BM> indexBloom,
        MiruIndexLatest<BM> indexLatest,
        MiruIndexPairedLatest<BM> indexPairedLatest) {
        this.indexAuthz = indexAuthz;
        this.indexFieldValues = indexFieldValues;
        this.indexBloom = indexBloom;
        this.indexLatest = indexLatest;
        this.indexPairedLatest = indexPairedLatest;
    }

    public void index(final MiruContext<BM, ?> context,
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
                context.activityInternExtern.intern(activityAndIds, startOfSubList, partitionSize, internalActivityAndIds, context.schema);
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
        List<Future<?>> fieldFutures = indexFieldValues.index(context, fieldValuesComposed, repair, indexExecutor);

        // 4. Wait for completion
        awaitFutures(fieldFutures, "indexFieldValues");

        // 5. Index remaining work
        final List<Future<?>> otherFutures = new ArrayList<>();
        otherFutures.addAll(indexAuthz.index(context, internalActivityAndIds, repair, indexExecutor));
        otherFutures.addAll(indexBloom.index(context, bloomPrepared, repair, indexExecutor));
        otherFutures.addAll(indexLatest.index(context, internalActivityAndIds, repair, indexExecutor));
        otherFutures.addAll(indexPairedLatest.index(context, pairedLatestPrepared, repair, indexExecutor));

        // 6. Update activity index
        otherFutures.add(indexExecutor.submit(() -> {
            byte[] primitiveBuffer = new byte[8];
            context.activityIndex.set(internalActivityAndIds, primitiveBuffer);
            return null;
        }));

        // 7. Update removal index
        if (repair) {
            otherFutures.add(indexExecutor.submit(() -> {
                byte[] primitiveBuffer = new byte[8];
                // repairs also unhide (remove from removal)
                for (MiruActivityAndId<MiruActivity> activityAndId : activityAndIds) {
                    context.removalIndex.remove(activityAndId.id, primitiveBuffer);
                }
                return null;
            }));
        }

        // 8. Wait for completion
        awaitFutures(otherFutures, "indexOther");

        // 9. Mark as ready
        byte[] primitiveBuffer = new byte[8];
        context.activityIndex.ready(internalActivityAndIds.get(internalActivityAndIds.size() - 1).id, primitiveBuffer);

        log.debug("End: Index batch of {}", internalActivityAndIds.size());
    }

    public void set(MiruContext<BM, ?> context, List<MiruActivityAndId<MiruActivity>> activityAndIds) throws Exception {
        @SuppressWarnings("unchecked")
        List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds = Arrays.<MiruActivityAndId<MiruInternalActivity>>asList(
            new MiruActivityAndId[activityAndIds.size()]);

        context.activityInternExtern.intern(activityAndIds, 0, activityAndIds.size(), internalActivityAndIds, context.schema);
        byte[] primitiveBuffer = new byte[8];
        context.activityIndex.setAndReady(internalActivityAndIds, primitiveBuffer);
    }

    /*
    public void repair(final MiruContext<BM> context,
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
    }
     */
    public void remove(MiruContext<BM, ?> context, MiruActivity activity, int id) throws Exception {
        byte[] primitiveBuffer = new byte[8];
        MiruInternalActivity existing = context.activityIndex.get(activity.tenantId, id, primitiveBuffer);
        if (existing == null) {
            log.debug("Can't remove nonexistent activity at {}\n- offered: {}", id, activity);
        } else if (activity.version <= existing.version) {
            log.debug("Declined to remove old activity at {}\n- have: {}\n- offered: {}", id, existing, activity);
        } else {
            log.debug("Removing activity at {}\n- was: {}\n- now: {}", id, existing, activity);
            @SuppressWarnings("unchecked")
            List<MiruActivityAndId<MiruInternalActivity>> internalActivity = Arrays.<MiruActivityAndId<MiruInternalActivity>>asList(
                new MiruActivityAndId[1]);
            context.activityInternExtern.intern(Arrays.asList(new MiruActivityAndId<>(activity, id)), 0, 1, internalActivity, context.schema);

            //TODO apply field changes?
            // hide (add to removal)
            context.removalIndex.set(primitiveBuffer, id);

            // finally, update the activity index
            context.activityIndex.setAndReady(internalActivity, primitiveBuffer);
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
