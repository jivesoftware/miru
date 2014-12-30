package com.jivesoftware.os.miru.manage.deployable.region.bean;

import com.google.common.collect.Lists;
import java.util.List;

/**
*
*/
public class PartitionBean {

    private final int partitionId;
    private final String activityCount;
    private final List<PartitionCoordBean> online = Lists.newArrayList();
    private final List<PartitionCoordBean> rebuilding = Lists.newArrayList();
    private final List<PartitionCoordBean> bootstrap = Lists.newArrayList();
    private final List<PartitionCoordBean> offline = Lists.newArrayList();

    public PartitionBean(int partitionId, long activityCount) {
        this.partitionId = partitionId;
        this.activityCount = String.valueOf(activityCount);
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getActivityCount() {
        return activityCount;
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
