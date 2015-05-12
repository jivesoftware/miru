package com.jivesoftware.os.miru.stream.plugins.filter;

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
public class AggregateCountsPlugin implements MiruPlugin<AggregateCountsEndpoints, AggregateCountsInjectable> {

    @Override
    public Class<AggregateCountsEndpoints> getEndpointsClass() {
        return AggregateCountsEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<AggregateCountsInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {

        ObjectMapper mapper = new ObjectMapper();
        JacksonMiruSolutionMarshaller<AggregateCountsQuery, AggregateCountsAnswer, AggregateCountsReport> marshaller = new JacksonMiruSolutionMarshaller<>(
            mapper,
            AggregateCountsQuery.class, AggregateCountsAnswer.class, AggregateCountsReport.class);

        AggregateCounts aggregateCounts = new AggregateCounts(miruProvider);
        return Collections.singletonList(new MiruEndpointInjectable<>(
            AggregateCountsInjectable.class,
            new AggregateCountsInjectable(miruProvider, aggregateCounts, marshaller)
        ));
    }
}
