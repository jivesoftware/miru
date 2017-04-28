package com.jivesoftware.os.miru.siphon.deployable.siphoner;

import java.util.Map;

/**
 * Created by jonathan.colt on 4/27/17.
 */
public interface AmzaSiphonerConfigProvider {
    Map<String, AmzaSiphonerConfig> getAll() throws Exception;
}
