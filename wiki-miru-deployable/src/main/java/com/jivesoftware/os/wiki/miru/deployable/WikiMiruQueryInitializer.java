package com.jivesoftware.os.wiki.miru.deployable;

import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.wiki.miru.deployable.region.MiruHomeRegion;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiMiruHeaderRegion;

public class WikiMiruQueryInitializer {

    public WikiMiruService initialize(MiruSoyRenderer renderer) throws Exception {

        return new WikiMiruService(
            renderer,
            new WikiMiruHeaderRegion("soy.wikimiru.chrome.headerRegion", renderer),
            new MiruHomeRegion("soy.wikimiru.page.home", renderer)
        );
    }

}
