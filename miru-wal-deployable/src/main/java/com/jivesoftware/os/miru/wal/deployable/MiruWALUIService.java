package com.jivesoftware.os.miru.wal.deployable;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.wal.deployable.region.MiruChromeRegion;
import com.jivesoftware.os.miru.wal.deployable.region.MiruFrameRegion;
import com.jivesoftware.os.miru.wal.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.wal.deployable.region.MiruManagePlugin;
import com.jivesoftware.os.miru.wal.deployable.region.input.MiruActivityWALRegionInput;
import com.jivesoftware.os.miru.wal.deployable.region.input.MiruReadWALRegionInput;
import java.util.List;

/**
 *
 */
public class MiruWALUIService {

    private final MiruSoyRenderer renderer;
    private final MiruHeaderRegion headerRegion;
    private final MiruPageRegion<Void> adminRegion;
    private final MiruPageRegion<MiruActivityWALRegionInput> activityWALRegion;
    private final MiruPageRegion<MiruReadWALRegionInput> readWALRegion;
    private final MiruPageRegion<Optional<String>> repairRegion;
    private final MiruPageRegion<Void> cleanupRegion;

    private final List<MiruManagePlugin> plugins = Lists.newCopyOnWriteArrayList();

    public MiruWALUIService(
        MiruSoyRenderer renderer,
        MiruHeaderRegion headerRegion,
        MiruPageRegion<Void> adminRegion,
        MiruPageRegion<MiruActivityWALRegionInput> activityWALRegion,
        MiruPageRegion<MiruReadWALRegionInput> readWALRegion,
        MiruPageRegion<Optional<String>> repairRegion,
        MiruPageRegion<Void> cleanupRegion) {
        this.renderer = renderer;
        this.headerRegion = headerRegion;
        this.adminRegion = adminRegion;
        this.activityWALRegion = activityWALRegion;
        this.readWALRegion = readWALRegion;
        this.repairRegion = repairRegion;
        this.cleanupRegion = cleanupRegion;
    }

    public void registerPlugin(MiruManagePlugin plugin) {
        plugins.add(plugin);
    }

    private <I, R extends MiruPageRegion<I>> MiruChromeRegion<I, R> chrome(R region) {
        return new MiruChromeRegion<>("soy.miru.chrome.chromeRegion", renderer, headerRegion, plugins, region);
    }

    private <I, R extends MiruPageRegion<I>> MiruFrameRegion<I, R> frame(R region) {
        return new MiruFrameRegion<>("soy.miru.frame.chromeRegion", renderer, region);
    }

    public String render(String redirUrl) {
        headerRegion.setRedirUrl(redirUrl);
        return chrome(adminRegion).render(null);
    }

    public String renderActivityWAL() {
        return chrome(activityWALRegion).render(new MiruActivityWALRegionInput(
            Optional.<MiruTenantId>absent(), Optional.<String>absent(), Optional.<MiruPartitionId>absent(), Optional.<Boolean>absent(),
            Optional.<Long>absent(), Optional.<Integer>absent()));
    }

    public String renderActivityWALWithTenant(MiruTenantId tenantId) {
        return chrome(activityWALRegion).render(new MiruActivityWALRegionInput(
            Optional.of(tenantId), Optional.<String>absent(), Optional.<MiruPartitionId>absent(), Optional.<Boolean>absent(), Optional.<Long>absent(),
            Optional.<Integer>absent()));
    }

    public String renderActivityWALWithFocus(MiruActivityWALRegionInput input) {
        return chrome(activityWALRegion).render(input);
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

    public String renderRepair() {
        return chrome(repairRegion).render(Optional.<String>absent());
    }

    public String renderRepairWithTenant(String tenantId) {
        return chrome(repairRegion).render(Optional.of(tenantId));
    }

    public String renderCleanup() {
        return chrome(cleanupRegion).render(null);
    }

}
