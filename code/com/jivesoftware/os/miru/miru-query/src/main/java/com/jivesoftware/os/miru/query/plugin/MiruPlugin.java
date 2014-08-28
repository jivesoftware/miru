package com.jivesoftware.os.miru.query.plugin;

import com.jivesoftware.os.miru.query.Miru;
import com.jivesoftware.os.miru.query.MiruProvider;
import java.util.Collection;

/**
 *
 */
public interface MiruPlugin<E, I> {

    Class<E> getEndpointsClass();

    Collection<MiruEndpointInjectable<I>> getInjectables(MiruProvider<? extends Miru> miruProvider);

}
