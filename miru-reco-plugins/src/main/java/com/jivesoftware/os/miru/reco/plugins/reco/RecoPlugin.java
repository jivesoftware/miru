package com.jivesoftware.os.miru.reco.plugins.reco;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.index.MiruIndexUtil;
import com.jivesoftware.os.miru.plugin.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.plugin.plugin.MiruPlugin;
import com.jivesoftware.os.miru.plugin.solution.JacksonMiruSolutionMarshaller;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.reco.plugins.distincts.Distincts;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsAnswer;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsReport;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class RecoPlugin implements MiruPlugin<RecoEndpoints, RecoInjectable> {

    @Override
    public Class<RecoEndpoints> getEndpointsClass() {
        return RecoEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<RecoInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {
        ObjectMapper mapper = new ObjectMapper();
        JacksonMiruSolutionMarshaller<DistinctsQuery, DistinctsAnswer, DistinctsReport> distinctsMarshaller = new JacksonMiruSolutionMarshaller<>(mapper,
            DistinctsQuery.class, DistinctsAnswer.class, DistinctsReport.class);

        JacksonMiruSolutionMarshaller<RecoQuery, RecoAnswer, RecoReport> recoMarshaller = new JacksonMiruSolutionMarshaller<>(mapper,
            RecoQuery.class, RecoAnswer.class, RecoReport.class);

        CollaborativeFiltering collaborativeFiltering = new CollaborativeFiltering(new MiruAggregateUtil(), new MiruIndexUtil());
        return Collections.singletonList(new MiruEndpointInjectable<>(
            RecoInjectable.class,
            new RecoInjectable(miruProvider, collaborativeFiltering, new Distincts(miruProvider.getTermComposer()), distinctsMarshaller, recoMarshaller)
        ));
    }
}
