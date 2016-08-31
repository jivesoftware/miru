package com.jivesoftware.os.miru.api.realtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import java.util.List;

/**
 *
 */
public class RealtimeUpdate {

    public final MiruHost miruHost;
    public final MiruPartitionCoord coord;
    public final List<Long> activityTimes;

    @JsonCreator
    public RealtimeUpdate(@JsonProperty("miruHost") MiruHost miruHost,
        @JsonProperty("coord") MiruPartitionCoord coord,
        @JsonProperty("activityTimes") List<Long> activityTimes) {
        this.miruHost = miruHost;
        this.coord = coord;
        this.activityTimes = activityTimes;
    }
}
