package com.jivesoftware.os.miru.plugin.plugin;

import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import java.util.Collection;

/**
 *
 */
public interface LifecycleMiruPlugin {

    void start(MiruProvider<? extends Miru> miruProvider) throws Exception;

    void stop(MiruProvider<? extends Miru> miruProvider);
}
