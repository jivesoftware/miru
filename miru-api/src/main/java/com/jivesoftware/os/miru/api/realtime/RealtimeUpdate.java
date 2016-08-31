package com.jivesoftware.os.miru.api.realtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import java.util.List;

/**
 *
 */
public class RealtimeUpdate {

    public final MiruPartitionCoord coord;
    public final List<Long> activityTimes;

    @JsonCreator
    public RealtimeUpdate(@JsonProperty("coord") MiruPartitionCoord coord,
        @JsonProperty("activityTimes") List<Long> activityTimes) {
        this.coord = coord;
        this.activityTimes = activityTimes;
    }
}
