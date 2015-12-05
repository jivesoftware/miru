package com.jivesoftware.os.miru.manage.deployable.region.bean;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.List;

/**
 *
 */
public class TenantBean {

    private final String tenantId;
    private final List<PartitionCoordBean> upgrading = Lists.newArrayList();
    private final List<PartitionCoordBean> online = Lists.newArrayList();
    private final List<PartitionCoordBean> rebuilding = Lists.newArrayList();
    private final List<PartitionCoordBean> bootstrap = Lists.newArrayList();
    private final List<PartitionCoordBean> offline = Lists.newArrayList();

    public TenantBean(MiruTenantId tenantId) {
        this.tenantId = new String(tenantId.getBytes(), Charsets.UTF_8);
    }

    public String getTenantId() {
        return tenantId;
    }

    public List<PartitionCoordBean> getUpgrading() {
        return upgrading;
    }

    public List<PartitionCoordBean> getOnline() {
        return online;
    }

    public List<PartitionCoordBean> getRebuilding() {
        return rebuilding;
    }

    public List<PartitionCoordBean> getBootstrap() {
        return bootstrap;
    }

    public List<PartitionCoordBean> getOffline() {
        return offline;
    }
}
