package com.jivesoftware.os.miru.plugin.plugin;

import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;

/**
 *
 */
public interface LifecycleMiruPlugin {

    void start(MiruProvider<? extends Miru> miruProvider) throws Exception;

    void stop(MiruProvider<? extends Miru> miruProvider);
}
