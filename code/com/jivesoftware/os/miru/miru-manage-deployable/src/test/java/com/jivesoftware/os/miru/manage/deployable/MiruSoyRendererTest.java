package com.jivesoftware.os.miru.manage.deployable;

import com.google.common.collect.ImmutableMap;
import com.google.template.soy.SoyFileSet;
import com.jivesoftware.os.miru.api.MiruHost;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class MiruSoyRendererTest {
    private MiruSoyRenderer renderer;

    @BeforeMethod
    public void setUp() throws Exception {
        SoyFileSet.Builder builder = new SoyFileSet.Builder();
        File soyFile = File.createTempFile("dumb", "soy");
        FileWriter writer = new FileWriter(soyFile);
        writer.write("{namespace miru}\n\n" +
                "/**\n" +
                " * @param host\n" +
                " */\n" +
                "{template .test}\n" +
                "{$host.logicalName}:{$host.port}\n" +
                "{/template}\n");
        writer.close();
        builder.add(soyFile);
        renderer = new MiruSoyRenderer(builder.build().compileToTofu(), new SoyDataUtils());
    }

    @Test
    public void testConvertObjectToMap() throws Exception {
        MiruHost host = new MiruHost("localhost", 10001);
        Map<String, ?> data = ImmutableMap.of("host", host);
        String rendered = renderer.render("miru.test", data);
        assertEquals(rendered, host.toStringForm());
    }
}
