package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class AnalyticsQuery implements Serializable {

    public final List<AnalyticsQueryScoreSet> scoreSets;
    public final MiruFilter constraintsFilter;
    public final Map<String, MiruFilter> analyticsFilters;

    @JsonCreator
    public AnalyticsQuery(
        @JsonProperty("scoreSets") List<AnalyticsQueryScoreSet> scoreSets,
        @JsonProperty("constraintsFilter") MiruFilter constraintsFilter,
        @JsonProperty("analyticsFilters") Map<String, MiruFilter> analyticsFilters) {
        this.scoreSets = Preconditions.checkNotNull(scoreSets);
        this.constraintsFilter = Preconditions.checkNotNull(constraintsFilter);
        this.analyticsFilters = Preconditions.checkNotNull(analyticsFilters);
    }

    @Override
    public String toString() {
        return "AnalyticsQuery{" +
            "scoreSets=" + scoreSets +
            ", constraintsFilter=" + constraintsFilter +
            ", analyticsFilters=" + analyticsFilters +
            '}';
    }
}
