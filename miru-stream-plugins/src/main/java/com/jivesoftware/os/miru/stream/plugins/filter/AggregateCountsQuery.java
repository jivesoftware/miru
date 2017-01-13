package com.jivesoftware.os.miru.stream.plugins.filter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.util.Map;

/**
 *
 */
public class AggregateCountsQuery {

    public final MiruStreamId streamId;
    public final MiruFilter suppressUnreadFilter;
    public final MiruTimeRange collectTimeRange;
    public final MiruTimeRange answerTimeRange;
    public final MiruTimeRange countTimeRange;
    public final MiruFilter streamFilter;
    public final Map<String, AggregateCountsQueryConstraint> constraints;
    public final boolean unreadOnly;

    public AggregateCountsQuery(
        @JsonProperty("streamId") MiruStreamId streamId,
        @JsonProperty("suppressUnreadFilter") MiruFilter suppressUnreadFilter,
        @JsonProperty("collectTimeRange") MiruTimeRange collectTimeRange,
        @JsonProperty("answerTimeRange") MiruTimeRange answerTimeRange,
        @JsonProperty("countTimeRange") MiruTimeRange countTimeRange,
        @JsonProperty("streamFilter") MiruFilter streamFilter,
        @JsonProperty("constraints") Map<String, AggregateCountsQueryConstraint> constraints,
        @JsonProperty("unreadOnly") boolean unreadOnly) {
        this.streamId = Preconditions.checkNotNull(streamId);
        this.suppressUnreadFilter = suppressUnreadFilter;
        this.collectTimeRange = Preconditions.checkNotNull(collectTimeRange);
        this.answerTimeRange = Preconditions.checkNotNull(answerTimeRange);
        this.countTimeRange = Preconditions.checkNotNull(countTimeRange);
        this.streamFilter = Preconditions.checkNotNull(streamFilter);
        this.constraints = Preconditions.checkNotNull(constraints);
        this.unreadOnly = unreadOnly;
    }

    @Override
    public String toString() {
        return "AggregateCountsQuery{" +
            "streamId=" + streamId +
            ", suppressUnreadFilter=" + suppressUnreadFilter +
            ", collectTimeRange=" + collectTimeRange +
            ", answerTimeRange=" + answerTimeRange +
            ", countTimeRange=" + countTimeRange +
            ", streamFilter=" + streamFilter +
            ", constraints=" + constraints +
            ", unreadOnly=" + unreadOnly +
            '}';
    }
}
