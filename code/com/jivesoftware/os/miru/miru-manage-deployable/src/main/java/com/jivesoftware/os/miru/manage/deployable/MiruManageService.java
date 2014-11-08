package com.jivesoftware.os.miru.manage.deployable;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.manage.deployable.region.MiruChromeRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruManagePlugin;
import com.jivesoftware.os.miru.manage.deployable.region.MiruPageRegion;
import com.jivesoftware.os.miru.manage.deployable.region.input.MiruActivityWALRegionInput;
import com.jivesoftware.os.miru.manage.deployable.region.input.MiruLookupRegionInput;
import com.jivesoftware.os.miru.manage.deployable.region.input.MiruReadWALRegionInput;
import java.util.List;

/**
 *
 */
public class MiruManageService {

    private final MiruSoyRenderer renderer;
    private final MiruHeaderRegion headerRegion;
    private final MiruPageRegion<Void> adminRegion;
    private final MiruPageRegion<Optional<MiruHost>> hostsRegion;
    private final MiruPageRegion<Void> balancerRegion;
    private final MiruPageRegion<Optional<MiruTenantId>> tenantsRegion;
    private final MiruPageRegion<MiruLookupRegionInput> lookupRegion;
    private final MiruPageRegion<MiruActivityWALRegionInput> activityWALRegion;
    private final MiruPageRegion<MiruReadWALRegionInput> readWALRegion;

    private final List<MiruManagePlugin> plugins = Lists.newCopyOnWriteArrayList();

    public MiruManageService(
        MiruSoyRenderer renderer,
        MiruHeaderRegion headerRegion,
        MiruPageRegion<Void> adminRegion,
        MiruPageRegion<Optional<MiruHost>> hostsRegion,
        MiruPageRegion<Void> balancerRegion,
        MiruPageRegion<Optional<MiruTenantId>> tenantsRegion,
        MiruPageRegion<MiruLookupRegionInput> lookupRegion,
        MiruPageRegion<MiruActivityWALRegionInput> activityWALRegion,
        MiruPageRegion<MiruReadWALRegionInput> readWALRegion) {
        this.renderer = renderer;
        this.headerRegion = headerRegion;
        this.adminRegion = adminRegion;
        this.hostsRegion = hostsRegion;
        this.balancerRegion = balancerRegion;
        this.tenantsRegion = tenantsRegion;
        this.lookupRegion = lookupRegion;
        this.activityWALRegion = activityWALRegion;
        this.readWALRegion = readWALRegion;
    }

    public void registerPlugin(MiruManagePlugin plugin) {
        plugins.add(plugin);
    }

    private <I, R extends MiruPageRegion<I>> MiruChromeRegion<I, R> chrome(R region) {
        return new MiruChromeRegion<>("soy.miru.chrome.chromeRegion", renderer, headerRegion, plugins, region);
    }

    public String render() {
        return chrome(adminRegion).render(null);
    }

    public String renderHosts() {
        return chrome(hostsRegion).render(Optional.<MiruHost>absent());
    }

    public String renderHostsWithFocus(MiruHost host) {
        return chrome(hostsRegion).render(Optional.of(host));
    }

    public String renderBalancer() {
        return chrome(balancerRegion).render(null);
    }

    public String renderTenants() {
        return chrome(tenantsRegion).render(Optional.<MiruTenantId>absent());
    }

    public String renderTenantsWithFocus(MiruTenantId tenantId) {
        return chrome(tenantsRegion).render(Optional.of(tenantId));
    }

    public String renderLookup() {
        return chrome(lookupRegion).render(new MiruLookupRegionInput(Optional.<MiruTenantId>absent(), Optional.<Long>absent(), Optional.<Integer>absent()));
    }

    public String renderLookupWithFocus(MiruTenantId tenantId, Optional<Long> afterTimestamp, Optional<Integer> limit) {
        return chrome(lookupRegion).render(new MiruLookupRegionInput(Optional.of(tenantId), afterTimestamp, limit));
    }

    public String renderActivityWAL() {
        return chrome(activityWALRegion).render(new MiruActivityWALRegionInput(
            Optional.<MiruTenantId>absent(), Optional.<MiruPartitionId>absent(), Optional.<Boolean>absent(),
            Optional.<Long>absent(), Optional.<Integer>absent()));
    }

    public String renderActivityWALWithTenant(MiruTenantId tenantId) {
        return chrome(activityWALRegion).render(new MiruActivityWALRegionInput(
            Optional.of(tenantId), Optional.<MiruPartitionId>absent(), Optional.<Boolean>absent(), Optional.<Long>absent(),
            Optional.<Integer>absent()));
    }

    public String renderActivityWALWithFocus(MiruTenantId tenantId, MiruPartitionId partitionId, Optional<Boolean> sip, Optional<Long> afterTimestamp,
        Optional<Integer> limit) {

        return chrome(activityWALRegion).render(new MiruActivityWALRegionInput(Optional.of(tenantId), Optional.of(partitionId), sip, afterTimestamp, limit));
    }

    public String renderReadWAL() {
        return chrome(readWALRegion).render(new MiruReadWALRegionInput(
            Optional.<MiruTenantId>absent(), Optional.<String>absent(), Optional.<Boolean>absent(), Optional.<Long>absent(),
            Optional.<Integer>absent()));
    }

    public String renderReadWALWithTenant(MiruTenantId tenantId) {
        return chrome(readWALRegion).render(new MiruReadWALRegionInput(
            Optional.of(tenantId), Optional.<String>absent(), Optional.<Boolean>absent(), Optional.<Long>absent(),
            Optional.<Integer>absent()));
    }

    public String renderReadWALWithFocus(MiruTenantId tenantId,
        String streamId,
        Optional<Boolean> sip,
        Optional<Long> afterTimestamp,
        Optional<Integer> limit) {
        return chrome(readWALRegion).render(new MiruReadWALRegionInput(Optional.of(tenantId), Optional.of(streamId), sip, afterTimestamp, limit));
    }

    public <I> String renderPlugin(MiruPageRegion<I> pluginRegion, I input) {
        return chrome(pluginRegion).render(input);
    }
}
