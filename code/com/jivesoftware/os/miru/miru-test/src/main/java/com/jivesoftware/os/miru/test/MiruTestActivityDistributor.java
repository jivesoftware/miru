package com.jivesoftware.os.miru.test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 *
 */
public class MiruTestActivityDistributor {

    private final Random random;

    private final MiruTestFeatureSupplier featureSupplier;
    private final float bgRepairPercent;
    private final float bgRemovePercent;
    private final Map<MiruTestActivityType, Integer> typeToActivityCount;

    private final int totalActivities;

    public MiruTestActivityDistributor(
        Random random,
        MiruTestFeatureSupplier featureSupplier,
        float bgRepairPercent,
        float bgRemovePercent,
        List<TypeAndCount> activityCounts,
        int totalActivities) {

        this.random = random;

        this.featureSupplier = featureSupplier;
        this.bgRepairPercent = bgRepairPercent;
        this.bgRemovePercent = bgRemovePercent;
        this.typeToActivityCount = Maps.newHashMap();
        this.totalActivities = totalActivities;

        for (TypeAndCount activityCount : activityCounts) {
            this.typeToActivityCount.put(activityCount.type, activityCount.count);
        }
    }

    public int getTotalActivities() {
        return totalActivities;
    }

    public MiruPartitionedActivity distribute(MiruPartitionedActivityFactory factory, int writerId, MiruPartitionId partitionId, int index,
        Optional<Revisitor> revisitor) throws IOException {

        if (revisitor.isPresent()) {
            if (random.nextFloat() < bgRepairPercent) {
                MiruPartitionedActivity partitionedActivity = revisitor.get().visit();
                // might be a boundary activity
                if (partitionedActivity.activity.isPresent()) {
                    MiruActivity oldActivity = partitionedActivity.activity.get();
                    String[] oldAuthz = oldActivity.authz;

                    String[] repairedAuthz = ImmutableSet.<String>builder()
                        .add(oldAuthz)
                        .add(featureSupplier.authz(featureSupplier.oldContainers(2).toArray(new Id[0])))
                        .build()
                        .toArray(new String[0]);
                    MiruActivity repairedActivity = new MiruActivity.Builder(oldActivity.tenantId, oldActivity.time, repairedAuthz, oldActivity.version + 1)
                        .putFieldsValues(oldActivity.fieldsValues)
                        .build();
                    return factory.repair(writerId, partitionedActivity.partitionId, partitionedActivity.index, repairedActivity);
                }
            } else if (random.nextFloat() < bgRemovePercent) {
                MiruPartitionedActivity partitionedActivity = revisitor.get().visit();
                // might be a boundary activity
                if (partitionedActivity.activity.isPresent()) {
                    MiruActivity oldActivity = partitionedActivity.activity.get();

                    MiruActivity removedActivity = new MiruActivity.Builder(oldActivity.tenantId, oldActivity.time, oldActivity.authz, oldActivity.version + 1)
                        .putFieldsValues(oldActivity.fieldsValues)
                        .build();
                    return factory.remove(writerId, partitionedActivity.partitionId, partitionedActivity.index, removedActivity);
                }
            } else {
                revisitor.get().skip();
            }
        }

        int r = random.nextInt(totalActivities);

        MiruTestActivityType type = null;
        int runningCount = 0;
        for (Map.Entry<MiruTestActivityType, Integer> entry : typeToActivityCount.entrySet()) {
            runningCount += entry.getValue();
            if (r < runningCount) {
                type = entry.getKey();
                break;
            }
        }

        if (type == null) {
            throw new RuntimeException("Failed to select an activity type");
        }

        return factory.activity(writerId, partitionId, index, type.generate(featureSupplier, featureSupplier.nextTimestamp()));
    }

    public static interface Revisitor {

        void skip() throws IOException;

        MiruPartitionedActivity visit() throws IOException;

        void close() throws IOException;
    }
}
