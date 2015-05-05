package com.jivesoftware.os.miru.manage.deployable.balancer;

import com.google.common.collect.ImmutableMap;
import com.google.template.soy.SoyFileSet;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.manage.deployable.SoyDataUtils;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class MiruSoyRendererTest {

    @Test
    public void testConvertMiruHost() throws Exception {
        SoyFileSet.Builder builder = new SoyFileSet.Builder();
        File soyFile = File.createTempFile("dumb", "soy");
        try (FileWriter writer = new FileWriter(soyFile)) {
            writer.write("{namespace miru}\n\n" +
                "/**\n" +
                " * @param host\n" +
                " */\n" +
                "{template .test}\n" +
                "{$host.logicalName}:{$host.port}\n" +
                "{/template}\n");
        }
        builder.add(soyFile);
        MiruSoyRenderer renderer = new MiruSoyRenderer(builder.build().compileToTofu(), new SoyDataUtils());

        MiruHost host = new MiruHost("localhost", 10_001);
        Map<String, ?> data = ImmutableMap.of("host", host);
        String rendered = renderer.render("miru.test", data);
        assertEquals(rendered, host.toStringForm());
    }

    @Test
    public void testConvertMiruBackingStorage() throws Exception {
        SoyFileSet.Builder builder = new SoyFileSet.Builder();
        File soyFile = File.createTempFile("dumb", "soy");
        try (FileWriter writer = new FileWriter(soyFile)) {
            writer.write("{namespace miru}\n\n" +
                "/**\n" +
                " * @param storage\n" +
                " */\n" +
                "{template .test}\n" +
                "{$storage}\n" +
                "{/template}\n");
        }
        builder.add(soyFile);
        MiruSoyRenderer renderer = new MiruSoyRenderer(builder.build().compileToTofu(), new SoyDataUtils());

        MiruBackingStorage storage = MiruBackingStorage.disk;
        Map<String, ?> data = ImmutableMap.of("storage", storage);
        String rendered = renderer.render("miru.test", data);
        assertEquals(rendered, storage.name());
    }

}
