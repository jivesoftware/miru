package com.jivesoftware.os.miru.plugin.plugin;

import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import java.util.Collection;

/**
 *
 */
public interface MiruPlugin<E, I> {

    Class<E> getEndpointsClass();

    Collection<MiruEndpointInjectable<I>> getInjectables(MiruProvider<? extends Miru> miruProvider);

    Collection<MiruRemotePartition<?, ?, ?>> getRemotePartitions(MiruProvider<? extends Miru> miruProvider);
}
