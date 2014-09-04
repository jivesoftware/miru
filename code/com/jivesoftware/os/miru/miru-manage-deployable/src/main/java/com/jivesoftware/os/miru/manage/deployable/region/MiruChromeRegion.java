package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import java.util.Map;

/**
 *
 */
// soy.miru.chrome.chromeRegion
public class MiruChromeRegion<I, R extends MiruPageRegion<I>> implements MiruRegion<I> {

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruHeaderRegion headerRegion;
    private final R region;
    private final String version;

    public MiruChromeRegion(String template, MiruSoyRenderer renderer, MiruHeaderRegion headerRegion, R region) {
        this.template = template;
        this.renderer = renderer;
        this.headerRegion = headerRegion;
        this.region = region;
        this.version = buildVersion();
    }

    @Override
    public String render(I input) {
        Map<String, Object> data = Maps.newHashMap();
        data.put("header", headerRegion.render(null));
        data.put("region", region.render(input));
        data.put("title", region.getTitle());
        data.put("version", version);
        return renderer.render(template, data);

        /*
        // inject js page region module data
        List<String> jsmodulesVal = Arrays.asList(JSProcessor.classToAMDPath(region.getClass()));

        context.put("jsmodules").value(jsmodulesVal);
        */
    }

    private String buildVersion() {
        /*
        GitInfo gitInfo = GitInfo.getInstance();
        String branch = gitInfo.getBranch();
        String branchSuffix = (gitInfo.getBranch().equals(gitInfo.getCommitId())) ? "" : "." + branch;
        return gitInfo.getBuildTime() + "." + gitInfo.getCommitIdAbbrev() + branchSuffix;
        */
        return "unknown";
    }

}
