package com.jivesoftware.os.miru.manage.deployable;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.manage.deployable.region.MiruRegion;
import com.jivesoftware.os.miru.manage.deployable.region.input.MiruActivityWALRegionInput;
import com.jivesoftware.os.miru.manage.deployable.region.input.MiruLookupRegionInput;
import com.jivesoftware.os.miru.manage.deployable.region.input.MiruReadWALRegionInput;

/**
 *
 */
public class MiruManageService {

    private final MiruRegion<Void> adminRegion;
    private final MiruRegion<Optional<MiruHost>> hostsRegion;
    private final MiruRegion<Void> balancerRegion;
    private final MiruRegion<Optional<MiruTenantId>> tenantsRegion;
    private final MiruRegion<MiruLookupRegionInput> lookupRegion;
    private final MiruRegion<MiruActivityWALRegionInput> activityWALRegion;
    private final MiruRegion<MiruReadWALRegionInput> readWALRegion;

    public MiruManageService(MiruRegion<Void> adminRegion,
        MiruRegion<Optional<MiruHost>> hostsRegion,
        MiruRegion<Void> balancerRegion,
        MiruRegion<Optional<MiruTenantId>> tenantsRegion,
        MiruRegion<MiruLookupRegionInput> lookupRegion,
        MiruRegion<MiruActivityWALRegionInput> activityWALRegion,
        MiruRegion<MiruReadWALRegionInput> readWALRegion) {
        this.adminRegion = adminRegion;
        this.hostsRegion = hostsRegion;
        this.balancerRegion = balancerRegion;
        this.tenantsRegion = tenantsRegion;
        this.lookupRegion = lookupRegion;
        this.activityWALRegion = activityWALRegion;
        this.readWALRegion = readWALRegion;
    }

    public String render() {
        return adminRegion.render(null);
    }

    public String renderHosts() {
        return hostsRegion.render(Optional.<MiruHost>absent());
    }

    public String renderHostsWithFocus(MiruHost host) {
        return hostsRegion.render(Optional.of(host));
    }

    public String renderBalancer() {
        return balancerRegion.render(null);
    }

    public String renderTenants() {
        return tenantsRegion.render(Optional.<MiruTenantId>absent());
    }

    public String renderTenantsWithFocus(MiruTenantId tenantId) {
        return tenantsRegion.render(Optional.of(tenantId));
    }

    public String renderLookup() {
        return lookupRegion.render(new MiruLookupRegionInput(Optional.<MiruTenantId>absent(), Optional.<Long>absent(), Optional.<Integer>absent()));
    }

    public String renderLookupWithFocus(MiruTenantId tenantId, Optional<Long> afterTimestamp, Optional<Integer> limit) {
        return lookupRegion.render(new MiruLookupRegionInput(Optional.of(tenantId), afterTimestamp, limit));
    }

    public String renderActivityWAL() {
        return activityWALRegion.render(new MiruActivityWALRegionInput(
            Optional.<MiruTenantId>absent(), Optional.<MiruPartitionId>absent(), Optional.<Boolean>absent(),
            Optional.<Long>absent(), Optional.<Integer>absent()));
    }

    public String renderActivityWALWithTenant(MiruTenantId tenantId) {
        return activityWALRegion.render(new MiruActivityWALRegionInput(
            Optional.of(tenantId), Optional.<MiruPartitionId>absent(), Optional.<Boolean>absent(), Optional.<Long>absent(),
            Optional.<Integer>absent()));
    }

    public String renderActivityWALWithFocus(MiruTenantId tenantId, MiruPartitionId partitionId, Optional<Boolean> sip, Optional<Long> afterTimestamp,
        Optional<Integer> limit) {

        return activityWALRegion.render(new MiruActivityWALRegionInput(Optional.of(tenantId), Optional.of(partitionId), sip, afterTimestamp, limit));
    }

    public String renderReadWAL() {
        return readWALRegion.render(new MiruReadWALRegionInput(
            Optional.<MiruTenantId>absent(), Optional.<String>absent(), Optional.<Boolean>absent(), Optional.<Long>absent(),
            Optional.<Integer>absent()));
    }

    public String renderReadWALWithTenant(MiruTenantId tenantId) {
        return readWALRegion.render(new MiruReadWALRegionInput(
            Optional.of(tenantId), Optional.<String>absent(), Optional.<Boolean>absent(), Optional.<Long>absent(),
            Optional.<Integer>absent()));
    }

    public String renderReadWALWithFocus(MiruTenantId tenantId,
        String streamId,
        Optional<Boolean> sip,
        Optional<Long> afterTimestamp,
        Optional<Integer> limit) {
        return readWALRegion.render(new MiruReadWALRegionInput(Optional.of(tenantId), Optional.of(streamId), sip, afterTimestamp, limit));
    }
}
