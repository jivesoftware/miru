package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.plugin.plugin.MiruPlugin;
import com.jivesoftware.os.miru.plugin.solution.JacksonMiruSolutionMarshaller;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class AnalyticsPlugin implements MiruPlugin<AnalyticsEndpoints, AnalyticsInjectable> {

    @Override
    public Class<AnalyticsEndpoints> getEndpointsClass() {
        return AnalyticsEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<AnalyticsInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {
        Analytics analytics = new Analytics();
        ObjectMapper mapper = new ObjectMapper();
        JacksonMiruSolutionMarshaller<AnalyticsQuery, AnalyticsAnswer, AnalyticsReport> marshaller = new JacksonMiruSolutionMarshaller<>(mapper,
            AnalyticsQuery.class, AnalyticsAnswer.class, AnalyticsReport.class);

        return Collections.singletonList(new MiruEndpointInjectable<>(
            AnalyticsInjectable.class,
            new AnalyticsInjectable(miruProvider, analytics, marshaller)
        ));
    }
}
