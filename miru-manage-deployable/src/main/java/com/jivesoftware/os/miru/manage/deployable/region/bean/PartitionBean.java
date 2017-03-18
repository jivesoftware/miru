package com.jivesoftware.os.miru.manage.deployable.region.bean;

import com.google.common.collect.Lists;
import java.util.List;

/**
 *
 */
public class PartitionBean {

    private final int partitionId;
    private String activityCount;
    private int begins;
    private int ends;
    private final List<PartitionCoordBean> upgrading = Lists.newArrayList();
    private final List<PartitionCoordBean> obsolete = Lists.newArrayList();
    private final List<PartitionCoordBean> online = Lists.newArrayList();
    private final List<PartitionCoordBean> rebuilding = Lists.newArrayList();
    private final List<PartitionCoordBean> bootstrap = Lists.newArrayList();
    private final List<PartitionCoordBean> offline = Lists.newArrayList();
    private boolean destroyed;
    private String minClock;
    private String maxClock;
    private String minOrderId;
    private String maxOrderId;
    private String errorMessage;

    public PartitionBean(int partitionId) {
        this.partitionId = partitionId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getActivityCount() {
        return activityCount;
    }

    public void setActivityCount(String activityCount) {
        this.activityCount = activityCount;
    }

    public int getBegins() {
        return begins;
    }

    public void setBegins(int begins) {
        this.begins = begins;
    }

    public int getEnds() {
        return ends;
    }

    public void setEnds(int ends) {
        this.ends = ends;
    }

    public List<PartitionCoordBean> getUpgrading() {
        return upgrading;
    }

    public List<PartitionCoordBean> getObsolete() {
        return obsolete;
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

    public boolean isDestroyed() {
        return destroyed;
    }

    public void setDestroyed(boolean destroyed) {
        this.destroyed = destroyed;
    }

    public String getMinClock() {
        return minClock;
    }

    public void setMinClock(String minClock) {
        this.minClock = minClock;
    }

    public String getMaxClock() {
        return maxClock;
    }

    public void setMaxClock(String maxClock) {
        this.maxClock = maxClock;
    }

    public String getMinOrderId() {
        return minOrderId;
    }

    public void setMinOrderId(String minOrderId) {
        this.minOrderId = minOrderId;
    }

    public String getMaxOrderId() {
        return maxOrderId;
    }

    public void setMaxOrderId(String maxOrderId) {
        this.maxOrderId = maxOrderId;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}
