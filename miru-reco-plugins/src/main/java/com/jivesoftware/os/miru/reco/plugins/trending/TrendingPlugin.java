package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.analytics.plugins.analytics.Analytics;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswer;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsQuery;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsReport;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.plugin.plugin.MiruPlugin;
import com.jivesoftware.os.miru.plugin.solution.JacksonMiruSolutionMarshaller;
import com.jivesoftware.os.miru.reco.plugins.distincts.Distincts;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsAnswer;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsReport;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class TrendingPlugin implements MiruPlugin<TrendingEndpoints, TrendingInjectable> {

    @Override
    public Class<TrendingEndpoints> getEndpointsClass() {
        return TrendingEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<TrendingInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {
        ObjectMapper mapper = new ObjectMapper();
        JacksonMiruSolutionMarshaller<DistinctsQuery, DistinctsAnswer, DistinctsReport> distinctsMarshaller = new JacksonMiruSolutionMarshaller<>(mapper,
            DistinctsQuery.class, DistinctsAnswer.class, DistinctsReport.class);

        JacksonMiruSolutionMarshaller<AnalyticsQuery, AnalyticsAnswer, AnalyticsReport> analyticsMarshaller = new JacksonMiruSolutionMarshaller<>(mapper,
            AnalyticsQuery.class, AnalyticsAnswer.class, AnalyticsReport.class);

        Distincts distincts = new Distincts(miruProvider.getTermComposer());
        Analytics analytics = new Analytics();
        return Collections.singletonList(new MiruEndpointInjectable<>(
            TrendingInjectable.class,
            new TrendingInjectable(miruProvider, distincts, analytics, distinctsMarshaller, analyticsMarshaller)
        ));
    }
}
