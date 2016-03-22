package com.jivesoftware.os.miru.plugin.context;

import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.map.MapContext;

/**
 *
 * @author jonathan.colt
 */
public interface MiruPluginCacheProvider {

    KeyedFilerStore<Integer, MapContext> get(String name);
}
