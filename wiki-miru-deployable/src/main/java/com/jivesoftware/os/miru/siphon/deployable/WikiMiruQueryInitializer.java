package com.jivesoftware.os.miru.siphon.deployable;

import com.jivesoftware.os.miru.siphon.deployable.region.WikiMiruHeaderRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.siphon.deployable.region.MiruHomeRegion;

public class WikiMiruQueryInitializer {

    public WikiMiruService initialize(MiruSoyRenderer renderer) throws Exception {

        return new WikiMiruService(
            renderer,
            new WikiMiruHeaderRegion("soy.wikimiru.chrome.headerRegion", renderer),
            new MiruHomeRegion("soy.wikimiru.page.home", renderer)
        );
    }

}
