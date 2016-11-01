package com.jivesoftware.os.miru.sync.deployable.region;

import com.jivesoftware.os.miru.ui.MiruRegion;

/**
 *
 */
public class MiruSyncPlugin {

    public final String name;
    public final String path;
    public final Class<?> endpointsClass;
    public final MiruRegion<?> region;
    public final MiruRegion<?>[] otherRegions;

    public MiruSyncPlugin(String name, String path, Class<?> endpointsClass, MiruRegion<?> region, MiruRegion<?>... otherRegions) {
        this.name = name;
        this.path = path;
        this.endpointsClass = endpointsClass;
        this.region = region;
        this.otherRegions = otherRegions;
    }
}
