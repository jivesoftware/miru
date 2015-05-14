package com.jivesoftware.os.miru.reco.plugins.distincts;

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
public class DistinctsPlugin implements MiruPlugin<DistinctsEndpoints, DistinctsInjectable> {

    @Override
    public Class<DistinctsEndpoints> getEndpointsClass() {
        return DistinctsEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<DistinctsInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {
        ObjectMapper mapper = new ObjectMapper();
        JacksonMiruSolutionMarshaller<DistinctsQuery, DistinctsAnswer, DistinctsReport> distinctsMarshaller = new JacksonMiruSolutionMarshaller<>(mapper,
            DistinctsQuery.class, DistinctsAnswer.class, DistinctsReport.class);

        Distincts distincts = new Distincts(miruProvider.getTermComposer());
        return Collections.singletonList(new MiruEndpointInjectable<>(
                DistinctsInjectable.class,
                new DistinctsInjectable(miruProvider, distincts, distinctsMarshaller)
        ));
    }
}
