package com.jivesoftware.os.miru.api.activity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import java.util.List;

/**
 *
 */
public class StreamIdPartitionedActivities {

    public final MiruStreamId streamId;
    public final List<MiruPartitionedActivity> partitionedActivities;

    @JsonCreator
    public StreamIdPartitionedActivities(@JsonProperty("streamId") byte[] streamId,
        @JsonProperty("partitionedActivities") List<MiruPartitionedActivity> partitionedActivities) {
        this.streamId = new MiruStreamId(streamId);
        this.partitionedActivities = partitionedActivities;
    }

    @JsonGetter("streamId")
    public byte[] getStreamIdAsBytes() {
        return streamId.getBytes();
    }

    @Override
    public String toString() {
        return "StreamIdPartitionedActivities{" +
            "streamId=" + streamId +
            ", partitionedActivities=" + partitionedActivities +
            '}';
    }
}
