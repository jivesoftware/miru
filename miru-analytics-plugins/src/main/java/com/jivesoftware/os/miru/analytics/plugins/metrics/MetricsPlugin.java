package com.jivesoftware.os.miru.analytics.plugins.metrics;

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
public class MetricsPlugin implements MiruPlugin<MetricsEndpoints, MetricsInjectable> {

    @Override
    public Class<MetricsEndpoints> getEndpointsClass() {
        return MetricsEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<MetricsInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {
        Metrics metrics = new Metrics();
        ObjectMapper mapper = new ObjectMapper();
        JacksonMiruSolutionMarshaller<MetricsQuery, MetricsAnswer, MetricsReport> marshaller = new JacksonMiruSolutionMarshaller<>(mapper,
            MetricsQuery.class, MetricsAnswer.class, MetricsReport.class);

        return Collections.singletonList(new MiruEndpointInjectable<>(
            MetricsInjectable.class,
            new MetricsInjectable(miruProvider, metrics, marshaller)
        ));
    }
}
