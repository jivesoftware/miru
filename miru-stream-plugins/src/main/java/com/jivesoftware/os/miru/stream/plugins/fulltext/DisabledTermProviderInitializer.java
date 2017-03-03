package com.jivesoftware.os.miru.stream.plugins.fulltext;

import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;

/**
 *
 */
public class DisabledTermProviderInitializer implements FullTextTermProviderInitializer<DisabledTermProvider> {

    @Override
    public DisabledTermProvider initialize(MiruProvider<? extends Miru> miruProvider) throws Exception {
        return new DisabledTermProvider();
    }
}
