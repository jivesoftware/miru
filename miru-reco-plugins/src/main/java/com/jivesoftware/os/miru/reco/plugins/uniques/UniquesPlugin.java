package com.jivesoftware.os.miru.reco.plugins.uniques;

import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.plugin.plugin.MiruPlugin;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.reco.plugins.distincts.Distincts;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class UniquesPlugin implements MiruPlugin<UniquesEndpoints, UniquesInjectable> {

    @Override
    public Class<UniquesEndpoints> getEndpointsClass() {
        return UniquesEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<UniquesInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {

        Distincts uniques = new Distincts(miruProvider.getTermComposer());
        return Collections.singletonList(new MiruEndpointInjectable<>(
            UniquesInjectable.class,
            new UniquesInjectable(miruProvider)
        ));
    }

    @Override
    public Collection<MiruRemotePartition<?, ?, ?>> getRemotePartitions(MiruProvider<? extends Miru> miruProvider) {
        return Collections.emptyList();
    }
}
