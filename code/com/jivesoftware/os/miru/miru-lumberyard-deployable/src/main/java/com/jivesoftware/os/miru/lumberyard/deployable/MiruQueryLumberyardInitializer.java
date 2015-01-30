package com.jivesoftware.os.miru.lumberyard.deployable;

import com.jivesoftware.os.miru.lumberyard.deployable.region.MiruAdminRegion;
import com.jivesoftware.os.miru.lumberyard.deployable.region.MiruHeaderRegion;
import org.merlin.config.Config;
import org.merlin.config.defaults.StringDefault;

public class MiruQueryLumberyardInitializer {

    public interface MiruQueryLumberyardConfig extends Config {

        @StringDefault("/miru/writer/client/ingress")
        public String getMiruIngressEndpoint();

        @StringDefault("unspecifiedHost:0")
        public String getMiruWriterHosts();

    }

    public MiruQueryLumberyardService initialize(MiruSoyRenderer renderer) throws Exception {

        return new MiruQueryLumberyardService(
            renderer,
            new MiruHeaderRegion("soy.miru.chrome.headerRegion", renderer),
            new MiruAdminRegion("soy.miru.page.adminRegion", renderer)
        );
    }

}
