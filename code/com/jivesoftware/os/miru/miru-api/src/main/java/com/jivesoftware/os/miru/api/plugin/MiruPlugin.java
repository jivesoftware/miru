package com.jivesoftware.os.miru.api.plugin;

import java.util.Collection;

/**
 *
 */
public interface MiruPlugin {

    Class<?> getEndpointsClass(Miru miru);

    Collection<MiruEndpointInjectable<?>> getInjectables();

}
