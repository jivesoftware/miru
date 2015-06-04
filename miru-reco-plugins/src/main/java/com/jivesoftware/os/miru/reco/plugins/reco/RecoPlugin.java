package com.jivesoftware.os.miru.reco.plugins.reco;

import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.index.MiruIndexUtil;
import com.jivesoftware.os.miru.plugin.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.plugin.plugin.MiruPlugin;
import com.jivesoftware.os.miru.plugin.solution.JsonRemotePartitionReader;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.reco.plugins.distincts.Distincts;
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
            new RecoInjectable(miruProvider, collaborativeFiltering, new Distincts(miruProvider.getTermComposer()))
        ));
    }

    @Override
    public Collection<MiruRemotePartition<?, ?, ?>> getRemotePartitions() {
        return Collections.singletonList(new RecoRemotePartition(new JsonRemotePartitionReader()));
    }
}
