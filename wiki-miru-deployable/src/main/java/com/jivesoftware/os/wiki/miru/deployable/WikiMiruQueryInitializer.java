package com.jivesoftware.os.wiki.miru.deployable;

import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.wiki.miru.deployable.region.MiruHomeRegion;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiMiruHeaderRegion;

public class WikiMiruQueryInitializer {

    public WikiMiruService initialize(MiruSoyRenderer renderer) throws Exception {

        return new WikiMiruService(
            renderer,
            new WikiMiruHeaderRegion("soy.wiki.chrome.headerReagion", renderer),
            new MiruHomeRegion("soy.wiki.page.home", renderer)
        );
    }

}
