package com.jivesoftware.os.miru.manage.deployable;

import com.google.template.soy.SoyFileSet;
import com.google.template.soy.tofu.SoyTofu;
import com.jivesoftware.os.miru.cluster.MiruActivityLookupTable;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSActivityLookupTable;
import com.jivesoftware.os.miru.manage.deployable.region.MiruActivityWALRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruAdminRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruChromeRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruHostEntryRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruHostFocusRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruHostsRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruLookupRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruReadWALRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruTenantEntryRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruTenantsRegion;
import com.jivesoftware.os.miru.wal.MiruWALInitializer.MiruWAL;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReaderImpl;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReaderImpl;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.regex.Pattern;
import org.merlin.config.Config;
import org.merlin.config.defaults.StringDefault;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

public class MiruManageInitializer {

    interface MiruManageConfig extends Config {

        @StringDefault("resources")
        String getPathToStaticResources();

        @StringDefault("soy.miru")
        String getSoyResourcePackage();
    }

    public MiruManageService initialize(MiruManageConfig config,
            MiruClusterRegistry clusterRegistry,
            MiruRegistryStore registryStore,
            MiruWAL miruWAL) throws Exception {

        Set<String> templateFilenames = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(config.getSoyResourcePackage()))
                .setScanners(new ResourcesScanner())).getResources(Pattern.compile(".*\\.soy"));

        SoyFileSet.Builder soyFileSetBuilder = new SoyFileSet.Builder();
        for (String fileName : templateFilenames) {
            URL url = getClass().getResource("/" + fileName);

            Path file = Paths.get(url.getFile());
            if (Files.exists(file)) {
                // file URL
                soyFileSetBuilder.add(file.toFile());
            }
        }

        SoyFileSet sfs = soyFileSetBuilder.build();
        SoyTofu tofu = sfs.compileToTofu();
        MiruSoyRenderer renderer = new MiruSoyRenderer(tofu);

        MiruActivityWALReader activityWALReader = new MiruActivityWALReaderImpl(miruWAL.getActivityWAL(), miruWAL.getActivitySipWAL());
        MiruReadTrackingWALReader readTrackingWALReader = new MiruReadTrackingWALReaderImpl(miruWAL.getReadTrackingWAL(), miruWAL.getReadTrackingSipWAL());
        MiruActivityLookupTable activityLookupTable = new MiruRCVSActivityLookupTable(registryStore.getActivityLookupTable());

        MiruHeaderRegion headerRegion = new MiruHeaderRegion("soy.miru.chrome.headerRegion", renderer);

        return new MiruManageService(
                new MiruChromeRegion<>("soy.miru.chrome.chromeRegion", renderer, headerRegion,
                        new MiruAdminRegion("soy.miru.page.adminRegion", renderer)),
                new MiruChromeRegion<>("soy.miru.chrome.chromeRegion", renderer, headerRegion,
                        new MiruHostsRegion("soy.miru.page.hostsRegion", renderer, clusterRegistry,
                        new MiruHostEntryRegion("soy.miru.section.hostEntryRegion", renderer, clusterRegistry),
                        new MiruHostFocusRegion("soy.miru.section.hostFocusRegion", renderer, clusterRegistry))),
                new MiruChromeRegion<>("soy.miru.chrome.chromeRegion", renderer, headerRegion,
                        new MiruTenantsRegion("soy.miru.page.tenantsRegion", renderer,
                        new MiruTenantEntryRegion("soy.miru.section.tenantEntryRegion", renderer, clusterRegistry))),
                new MiruChromeRegion<>("soy.miru.chrome.chromeRegion", renderer, headerRegion,
                        new MiruLookupRegion("soy.miru.page.lookupRegion", renderer, activityLookupTable)),
                new MiruChromeRegion<>("soy.miru.chrome.chromeRegion", renderer, headerRegion,
                        new MiruActivityWALRegion("soy.miru.page.activityWalRegion", renderer, clusterRegistry, activityWALReader)),
                new MiruChromeRegion<>("soy.miru.chrome.chromeRegion", renderer, headerRegion,
                        new MiruReadWALRegion("soy.miru.page.readWalRegion", renderer, readTrackingWALReader)));
    }
}
