package com.jivesoftware.os.miru.stream.plugins.count;

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
public class DistinctCountPlugin implements MiruPlugin<DistinctCountEndpoints, DistinctCountInjectable> {

    @Override
    public Class<DistinctCountEndpoints> getEndpointsClass() {
        return DistinctCountEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<DistinctCountInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {
        ObjectMapper mapper = new ObjectMapper();
        JacksonMiruSolutionMarshaller<DistinctCountQuery, DistinctCountAnswer, DistinctCountReport> marshaller = new JacksonMiruSolutionMarshaller<>(mapper,
            DistinctCountQuery.class, DistinctCountAnswer.class, DistinctCountReport.class);

        DistinctCount distinctCount = new DistinctCount();
        return Collections.singletonList(new MiruEndpointInjectable<>(
            DistinctCountInjectable.class,
            new DistinctCountInjectable(miruProvider, distinctCount, marshaller)
        ));
    }
}
