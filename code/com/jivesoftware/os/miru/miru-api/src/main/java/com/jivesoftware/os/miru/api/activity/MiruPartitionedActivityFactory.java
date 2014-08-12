package com.jivesoftware.os.miru.api.activity;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public class MiruPartitionedActivityFactory {

    private final ClockTimestamper timestamper;

    public MiruPartitionedActivityFactory() {
        this(new ClockTimestamper() {
            @Override
            public long get() {
                return System.currentTimeMillis();
            }
        });
    }

    public MiruPartitionedActivityFactory(ClockTimestamper timestamper) {
        this.timestamper = timestamper;
    }

    public MiruPartitionedActivity begin(int writerId, MiruPartitionId partitionId, MiruTenantId tenantId, int lastIndex) {
        return new MiruPartitionedActivity(
            MiruPartitionedActivity.Type.BEGIN,
            writerId,
            partitionId,
            tenantId,
            lastIndex,
            Long.MAX_VALUE,
            timestamper.get(),
            Optional.<MiruActivity>absent(),
            Optional.<MiruReadEvent>absent());
    }

    public MiruPartitionedActivity end(int writerId, MiruPartitionId partitionId, MiruTenantId tenantId, int lastIndex) {
        return new MiruPartitionedActivity(
            MiruPartitionedActivity.Type.END,
            writerId,
            partitionId,
            tenantId,
            lastIndex,
            Long.MAX_VALUE,
            timestamper.get(),
            Optional.<MiruActivity>absent(),
            Optional.<MiruReadEvent>absent());
    }

    public MiruPartitionedActivity activity(int writerId, MiruPartitionId partitionId, int index, MiruActivity activity) {
        return new MiruPartitionedActivity(
            MiruPartitionedActivity.Type.ACTIVITY,
            writerId,
            partitionId,
            activity.tenantId,
            index,
            activity.time,
            timestamper.get(),
            Optional.of(activity),
            Optional.<MiruReadEvent>absent());
    }

    public MiruPartitionedActivity repair(int writerId, MiruPartitionId partitionId, int index, MiruActivity activity) {
        return new MiruPartitionedActivity(
            MiruPartitionedActivity.Type.REPAIR,
            writerId,
            partitionId,
            activity.tenantId,
            index,
            activity.time,
            timestamper.get(),
            Optional.of(activity),
            Optional.<MiruReadEvent>absent());
    }

    public MiruPartitionedActivity remove(int writerId, MiruPartitionId partitionId, int index, MiruActivity activity) {
        return new MiruPartitionedActivity(
            MiruPartitionedActivity.Type.REMOVE,
            writerId,
            partitionId,
            activity.tenantId,
            index,
            activity.time,
            timestamper.get(),
            Optional.of(activity),
            Optional.<MiruReadEvent>absent());
    }

    public MiruPartitionedActivity read(int writerId, MiruPartitionId partitionId, int index, MiruReadEvent readEvent) {
        return new MiruPartitionedActivity(
            MiruPartitionedActivity.Type.READ,
            writerId,
            partitionId,
            readEvent.tenantId,
            index,
            readEvent.time,
            timestamper.get(),
            Optional.<MiruActivity>absent(),
            Optional.of(readEvent));
    }

    public MiruPartitionedActivity unread(int writerId, MiruPartitionId partitionId, int index, MiruReadEvent readEvent) {
        return new MiruPartitionedActivity(
            MiruPartitionedActivity.Type.UNREAD,
            writerId,
            partitionId,
            readEvent.tenantId,
            index,
            readEvent.time,
            timestamper.get(),
            Optional.<MiruActivity>absent(),
            Optional.of(readEvent));
    }

    public MiruPartitionedActivity allread(int writerId, MiruPartitionId partitionId, int index, MiruReadEvent readEvent) {
        return new MiruPartitionedActivity(
            MiruPartitionedActivity.Type.MARK_ALL_READ,
            writerId,
            partitionId,
            readEvent.tenantId,
            index,
            readEvent.time,
            timestamper.get(),
            Optional.<MiruActivity>absent(),
            Optional.of(readEvent));
    }

    public static interface ClockTimestamper {
        long get();
    }

}
