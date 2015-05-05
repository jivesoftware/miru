package com.jivesoftware.os.miru.reader.deployable;

/**
 *
 */
public class MiruReaderUIPlugin {

    public final String glyphicon;
    public final String name;
    public final String path;
    public final Class<?> endpointsClass;
    public final MiruRegion<?> region;
    public final MiruRegion<?>[] otherRegions;

    public MiruReaderUIPlugin(String glyphicon, String name, String path, Class<?> endpointsClass, MiruRegion<?> region, MiruRegion<?>... otherRegions) {
        this.glyphicon = glyphicon;
        this.name = name;
        this.path = path;
        this.endpointsClass = endpointsClass;
        this.region = region;
        this.otherRegions = otherRegions;
    }
}
