package com.jivesoftware.os.miru.stream.plugins.filter;

import com.jivesoftware.os.miru.query.Miru;
import com.jivesoftware.os.miru.query.MiruProvider;
import com.jivesoftware.os.miru.query.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.query.plugin.MiruPlugin;
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
        AggregateCounts aggregateCounts = new AggregateCounts(miruProvider);
        return Collections.singletonList(new MiruEndpointInjectable<>(
                AggregateCountsInjectable.class,
                new AggregateCountsInjectable(miruProvider, aggregateCounts)
        ));
    }
}
