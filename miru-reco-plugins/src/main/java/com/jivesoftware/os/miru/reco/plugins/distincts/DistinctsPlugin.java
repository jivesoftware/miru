package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.plugin.plugin.MiruPlugin;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.plugin.solution.SnappyJsonRemotePartitionReader;
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

        Distincts distincts = new Distincts(miruProvider.getTermComposer());
        return Collections.singletonList(new MiruEndpointInjectable<>(
            DistinctsInjectable.class,
            new DistinctsInjectable(miruProvider, distincts)
        ));
    }

    @Override
    public Collection<MiruRemotePartition<?, ?, ?>> getRemotePartitions() {
        return Collections.singletonList(new DistinctsRemotePartition(new SnappyJsonRemotePartitionReader()));
    }
}
