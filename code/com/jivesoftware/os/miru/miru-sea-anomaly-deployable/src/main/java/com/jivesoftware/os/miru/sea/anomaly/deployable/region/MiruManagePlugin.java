package com.jivesoftware.os.miru.sea.anomaly.deployable.region;

/**
 *
 */
public class MiruManagePlugin {

    public final String name;
    public final String path;
    public final Class<?> endpointsClass;
    public final Region<?> region;

    public MiruManagePlugin(String name, String path, Class<?> endpointsClass, Region<?> region) {
        this.name = name;
        this.path = path;
        this.endpointsClass = endpointsClass;
        this.region = region;
    }
}
