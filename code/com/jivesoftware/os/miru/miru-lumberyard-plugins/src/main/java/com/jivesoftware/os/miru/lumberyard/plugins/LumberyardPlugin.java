package com.jivesoftware.os.miru.lumberyard.plugins;

import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.plugin.plugin.MiruPlugin;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class LumberyardPlugin implements MiruPlugin<LumberyardEndpoints, LumberyardInjectable> {

    @Override
    public Class<LumberyardEndpoints> getEndpointsClass() {
        return LumberyardEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<LumberyardInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {
        Lumberyard lumberyard = new Lumberyard(miruProvider);
        return Collections.singletonList(new MiruEndpointInjectable<>(
            LumberyardInjectable.class,
            new LumberyardInjectable(miruProvider, lumberyard)
        ));
    }
}
