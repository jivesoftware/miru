package com.jivesoftware.os.miru.writer.deployable;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import java.util.List;
import org.merlin.config.BindInterfaceToConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class MiruWriterUIServiceNGTest {

    private MiruWriterUIService service;
    private MiruTenantId tenantId;
    private List<MiruHost> hosts;
    private MiruPartitionId partitionId;

    @BeforeClass
    public void before() throws Exception {
        tenantId = new MiruTenantId("test1".getBytes());
        partitionId = MiruPartitionId.of(0);
        hosts = Lists.newArrayList();

        MiruSoyRendererConfig config = BindInterfaceToConfiguration.bindDefault(MiruSoyRendererConfig.class);
        config.setPathToSoyResources("src/main/home/resources/soy");

        MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(config);
        service = new MiruWriterUIServiceInitializer().initialize("test", 1, renderer, new MiruStats(), null);
    }

    @Test(enabled = false)
    public void testRenderActivityWALWithTenant() throws Exception {
        //TODO something
    }

}
