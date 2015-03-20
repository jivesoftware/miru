package com.jivesoftware.os.miru.reader.deployable;


public class MiruReaderUIInitializer {

    public MiruReaderUIService initialize(MiruSoyRenderer renderer) throws Exception {

        return new MiruReaderUIService(
            renderer,
            new MiruHeaderRegion("soy.miru.chrome.headerRegion", renderer),
            new MiruAdminRegion("soy.miru.page.adminRegion", renderer));
    }
}
