package com.jivesoftware.os.miru.manage.deployable;

import com.google.common.base.Preconditions;
import com.google.template.soy.tofu.SoyTofu;
import java.util.Map;

/**
 *
 */
public class MiruSoyRenderer {

    private final SoyTofu tofu;

    public MiruSoyRenderer(SoyTofu tofu) {
        this.tofu = tofu;
    }

    public String render(String template, Map<String, ?> data) {
        Preconditions.checkArgument(template != null && !template.isEmpty(), "argument is null or empty [template]");
        Preconditions.checkNotNull(data, "argument is null [data]");

        SoyTofu.Renderer renderer = tofu.newRenderer(template);
        if (renderer == null) {
            throw new IllegalArgumentException("No renderer found for template : " + template + ".  Are you sure it exists?");
        }

        renderer.setData(data);

        return renderer.render();
    }
}
