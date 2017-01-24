package com.jivesoftware.os.miru.stream.plugins.fulltext;

import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;

/**
 *
 */
public interface FullTextTermProviderInitializer<T extends FullTextTermProvider> {

    T initialize(MiruProvider<? extends Miru> miruProvider) throws Exception;
}
