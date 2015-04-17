package com.jivesoftware.os.miru.writer.deployable;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.writer.deployable.region.MiruChromeRegion;
import com.jivesoftware.os.miru.writer.deployable.region.MiruFrameRegion;
import com.jivesoftware.os.miru.writer.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.writer.deployable.region.MiruManagePlugin;
import com.jivesoftware.os.miru.writer.deployable.region.MiruPageRegion;
import com.jivesoftware.os.miru.writer.deployable.region.input.MiruActivityWALRegionInput;
import com.jivesoftware.os.miru.writer.deployable.region.input.MiruLookupRegionInput;
import com.jivesoftware.os.miru.writer.deployable.region.input.MiruReadWALRegionInput;
import java.util.List;

/**
 *
 */
public class MiruWriterUIService {

    private final MiruSoyRenderer renderer;
    private final MiruHeaderRegion headerRegion;
    private final MiruPageRegion<Void> adminRegion;
    private final MiruPageRegion<MiruLookupRegionInput> lookupRegion;
    private final MiruPageRegion<MiruActivityWALRegionInput> activityWALRegion;
    private final MiruPageRegion<MiruReadWALRegionInput> readWALRegion;
    private final MiruPageRegion<Optional<String>> repairRegion;

    private final List<MiruManagePlugin> plugins = Lists.newCopyOnWriteArrayList();

    public MiruWriterUIService(
        MiruSoyRenderer renderer,
        MiruHeaderRegion headerRegion,
        MiruPageRegion<Void> adminRegion,
        MiruPageRegion<MiruLookupRegionInput> lookupRegion,
        MiruPageRegion<MiruActivityWALRegionInput> activityWALRegion,
        MiruPageRegion<MiruReadWALRegionInput> readWALRegion,
        MiruPageRegion<Optional<String>> repairRegion) {
        this.renderer = renderer;
        this.headerRegion = headerRegion;
        this.adminRegion = adminRegion;
        this.lookupRegion = lookupRegion;
        this.activityWALRegion = activityWALRegion;
        this.readWALRegion = readWALRegion;
        this.repairRegion = repairRegion;
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

    public String render() {
        return chrome(adminRegion).render(null);
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

    public String renderRepair() {
        return chrome(repairRegion).render(Optional.<String>absent());
    }

    public String renderRepairWithTenant(String tenantId) {
        return chrome(repairRegion).render(Optional.of(tenantId));
    }

    public <I> String renderPlugin(MiruPageRegion<I> pluginRegion, I input) {
        return chrome(pluginRegion).render(input);
    }

    public <I> String renderFramePlugin(MiruPageRegion<I> pluginRegion, I input) {
        return frame(pluginRegion).render(input);
    }

}
