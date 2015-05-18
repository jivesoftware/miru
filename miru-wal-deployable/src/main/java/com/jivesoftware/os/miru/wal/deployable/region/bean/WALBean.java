package com.jivesoftware.os.miru.wal.deployable.region.bean;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class WALBean {

    private final String collisionId;
    private final MiruPartitionedActivity.Type type;
    private final int writerId;
    private final int index;
    private final String timestamp;
    private final String version;

    private String activityTime;
    private List<String> activityAuthz;
    private Map<String, List<String>> activityFieldsValues;
    private Map<String, List<String>> activityPropsValues;

    private String readTime;
    private String readStreamId;
    private String readFilter;

    public WALBean(long collisionId, Optional<MiruPartitionedActivity> partitionedActivity, long version) {
        this.collisionId = String.valueOf(collisionId);
        this.type = partitionedActivity.isPresent() ? partitionedActivity.get().type : null;
        this.writerId = partitionedActivity.isPresent() ? partitionedActivity.get().writerId : -1;
        this.index = partitionedActivity.isPresent() ? partitionedActivity.get().index : -1;
        this.timestamp = partitionedActivity.isPresent() ? String.valueOf(partitionedActivity.get().timestamp) : null;
        this.version = String.valueOf(version);

        if (partitionedActivity.isPresent() && partitionedActivity.get().activity.isPresent()) {
            MiruActivity miruActivity = partitionedActivity.get().activity.get();
            this.activityTime = String.valueOf(miruActivity.time);
            this.activityAuthz = miruActivity.authz != null ? Lists.newArrayList(miruActivity.authz) : Lists.<String>newArrayList();
            this.activityFieldsValues = miruActivity.fieldsValues;
            this.activityPropsValues = miruActivity.propsValues;
        }

        if (partitionedActivity.isPresent() && partitionedActivity.get().readEvent.isPresent()) {
            MiruReadEvent miruReadEvent = partitionedActivity.get().readEvent.get();
            this.readTime = String.valueOf(miruReadEvent.time);
            this.readStreamId = new Id(miruReadEvent.streamId.getBytes()).toStringForm();
            this.readFilter = miruReadEvent.filter.toString();
        }
    }

    public String getCollisionId() {
        return collisionId;
    }

    public MiruPartitionedActivity.Type getType() {
        return type;
    }

    public int getWriterId() {
        return writerId;
    }

    public int getIndex() {
        return index;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getVersion() {
        return version;
    }

    public String getActivityTime() {
        return activityTime;
    }

    public List<String> getActivityAuthz() {
        return activityAuthz;
    }

    public Map<String, List<String>> getActivityFieldsValues() {
        return activityFieldsValues;
    }

    public Map<String, List<String>> getActivityPropsValues() {
        return activityPropsValues;
    }

    public String getReadTime() {
        return readTime;
    }

    public String getReadStreamId() {
        return readStreamId;
    }

    public String getReadFilter() {
        return readFilter;
    }
}
