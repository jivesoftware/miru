package com.jivesoftware.os.miru.sea.anomaly.plugins;

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
public class SeaAnomalyPlugin implements MiruPlugin<SeaAnomalyEndpoints, SeaAnomalyInjectable> {

    @Override
    public Class<SeaAnomalyEndpoints> getEndpointsClass() {
        return SeaAnomalyEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<SeaAnomalyInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {

        ObjectMapper mapper = new ObjectMapper();
        JacksonMiruSolutionMarshaller<SeaAnomalyQuery, SeaAnomalyAnswer, SeaAnomalyReport> marshaller = new JacksonMiruSolutionMarshaller<>(mapper,
            SeaAnomalyQuery.class, SeaAnomalyAnswer.class, SeaAnomalyReport.class);

        SeaAnomaly stumptown = new SeaAnomaly(miruProvider);
        return Collections.singletonList(new MiruEndpointInjectable<>(
            SeaAnomalyInjectable.class,
            new SeaAnomalyInjectable(miruProvider, stumptown, marshaller)
        ));
    }
}
