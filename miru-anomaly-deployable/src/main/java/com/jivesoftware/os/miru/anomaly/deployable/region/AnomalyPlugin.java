package com.jivesoftware.os.miru.anomaly.deployable.region;

import com.jivesoftware.os.miru.ui.MiruRegion;

/**
 *
 */
public class AnomalyPlugin {

    public final String glyphicon;
    public final String name;
    public final String path;
    public final Class<?> endpointsClass;
    public final MiruRegion<?> region;

    public AnomalyPlugin(String glyphicon, String name, String path, Class<?> endpointsClass, MiruRegion<?> region) {
        this.glyphicon = glyphicon;
        this.name = name;
        this.path = path;
        this.endpointsClass = endpointsClass;
        this.region = region;
    }
}
