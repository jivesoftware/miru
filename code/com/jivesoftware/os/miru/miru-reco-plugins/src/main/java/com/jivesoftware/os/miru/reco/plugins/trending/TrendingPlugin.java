package com.jivesoftware.os.miru.reco.plugins.trending;

import com.jivesoftware.os.miru.query.Miru;
import com.jivesoftware.os.miru.query.MiruProvider;
import com.jivesoftware.os.miru.query.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.query.plugin.MiruPlugin;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class TrendingPlugin implements MiruPlugin<TrendingEndpoints, TrendingInjectable> {

    @Override
    public Class<TrendingEndpoints> getEndpointsClass() {
        return TrendingEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<TrendingInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {
        Trending trending = new Trending();
        return Collections.singletonList(new MiruEndpointInjectable<>(
                TrendingInjectable.class,
                new TrendingInjectable(miruProvider, trending)
        ));
    }
}
