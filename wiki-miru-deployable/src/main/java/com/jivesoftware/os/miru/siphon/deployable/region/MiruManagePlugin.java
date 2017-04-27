package com.jivesoftware.os.miru.siphon.deployable.region;

import com.jivesoftware.os.miru.ui.MiruRegion;

/**
 *
 */
public class MiruManagePlugin {

    public final String glyphicon;
    public final String name;
    public final String path;
    public final Class<?> endpointsClass;
    public final MiruRegion<?> region;

    public MiruManagePlugin(String glyphicon, String name, String path, Class<?> endpointsClass, MiruRegion<?> region) {
        this.glyphicon = glyphicon;
        this.name = name;
        this.path = path;
        this.endpointsClass = endpointsClass;
        this.region = region;
    }
}
