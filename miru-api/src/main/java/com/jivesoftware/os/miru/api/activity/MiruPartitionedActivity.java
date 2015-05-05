package com.jivesoftware.os.miru.api.activity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/** @author jonathan */
public class MiruPartitionedActivity {

    static public enum Type {

        // sort maintains things in lex descending order
        BEGIN(0, true, false, false),
        END(1, true, false, false),
        // these 5 sort equally
        ACTIVITY(2, false, true, false),
        REPAIR(2, false, true, false),
        REMOVE(2, false, true, false),
        READ(2, false, false, true),
        UNREAD(2, false, false, true),
        MARK_ALL_READ(2, false, false, true);

        private final byte sort;
        private final boolean boundaryType;
        private final boolean activityType;
        private final boolean readType;

        private Type(int reverseSort, boolean boundaryType, boolean activityType, boolean readType) {
            this.boundaryType = boundaryType;
            this.activityType = activityType;
            this.readType = readType;
            this.sort = (byte) (Byte.MAX_VALUE - reverseSort);
        }

        public byte getSort() {
            return sort;
        }

        public boolean isBoundaryType() {
            return boundaryType;
        }

        public boolean isActivityType() {
            return activityType;
        }

        public boolean isReadType() {
            return readType;
        }
    }

    public final Type type;
    public final int writerId;
    public final MiruPartitionId partitionId;
    public final MiruTenantId tenantId;
    public final int index;
    public final long timestamp;
    public final long clockTimestamp;
    public final Optional<MiruActivity> activity;
    public final Optional<MiruReadEvent> readEvent;

    /** Construct using {@link MiruPartitionedActivityFactory}. */
    MiruPartitionedActivity(Type type, int writerId, MiruPartitionId partitionId, MiruTenantId tenantId, int index, long timestamp,
        long clockTimestamp, Optional<MiruActivity> activity, Optional<MiruReadEvent> readEvent) {
        this.type = type;
        this.writerId = writerId;
        this.partitionId = partitionId;
        this.tenantId = tenantId;
        this.index = index;
        this.timestamp = timestamp;
        this.clockTimestamp = clockTimestamp;
        this.activity = activity;
        this.readEvent = readEvent;
    }

    /** Subverts the package constructor, but at least you'll feel dumb using a method called <code>fromJson</code>. */
    @JsonCreator
    public static MiruPartitionedActivity fromJson(
        @JsonProperty("type") Type type,
        @JsonProperty("writerId") int writerId,
        @JsonProperty("partitionId") int partitionId,
        @JsonProperty("tenantId") byte[] tenantId,
        @JsonProperty("index") int index,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("clock") long clockTimestamp,
        @JsonProperty("activity") MiruActivity activity,
        @JsonProperty("readEvent") MiruReadEvent readEvent) {
        return new MiruPartitionedActivity(type, writerId, MiruPartitionId.of(partitionId), new MiruTenantId(tenantId), index, timestamp,
            clockTimestamp, Optional.fromNullable(activity), Optional.fromNullable(readEvent));
    }

    @JsonGetter("activity")
    public MiruActivity getActivityNullable() {
        return activity.orNull();
    }

    @JsonGetter("readEvent")
    public MiruReadEvent getReadEventNullable() {
        return readEvent.orNull();
    }

    @JsonGetter("partitionId")
    public int getPartitionId() {
        return partitionId.getId();
    }

    @JsonGetter("tenantId")
    public byte[] getTenantIdAsBytes() {
        return tenantId.getBytes();
    }
}
