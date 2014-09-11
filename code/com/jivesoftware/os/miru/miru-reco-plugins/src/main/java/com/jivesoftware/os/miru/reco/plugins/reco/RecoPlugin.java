package com.jivesoftware.os.miru.reco.plugins.reco;

import com.jivesoftware.os.miru.query.Miru;
import com.jivesoftware.os.miru.query.MiruProvider;
import com.jivesoftware.os.miru.query.index.MiruIndexUtil;
import com.jivesoftware.os.miru.query.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.query.plugin.MiruPlugin;
import com.jivesoftware.os.miru.query.solution.MiruAggregateUtil;
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
        CollaborativeFiltering collaborativeFiltering = new CollaborativeFiltering(new MiruAggregateUtil(), new MiruIndexUtil());
        return Collections.singletonList(new MiruEndpointInjectable<>(
                RecoInjectable.class,
                new RecoInjectable(miruProvider, collaborativeFiltering)
        ));
    }
}
