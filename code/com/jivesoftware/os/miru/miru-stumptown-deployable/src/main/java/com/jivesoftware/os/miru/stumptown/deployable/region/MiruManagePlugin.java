package com.jivesoftware.os.miru.stumptown.deployable.region;

/**
 *
 */
public class MiruManagePlugin {

    public final String name;
    public final String path;
    public final Class<?> endpointsClass;
    public final MiruRegion<?> region;

    public MiruManagePlugin(String name, String path, Class<?> endpointsClass, MiruRegion<?> region) {
        this.name = name;
        this.path = path;
        this.endpointsClass = endpointsClass;
        this.region = region;
    }
}
