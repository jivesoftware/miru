package com.jivesoftware.os.miru.api.activity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;

public class MiruStreamEvent {

    public final MiruTenantId tenantId;
    public final long time;
    public final String streamName;
    public final String type;
    public final MiruStreamId streamId;
    public final MiruFilter filter;

    @JsonCreator
    public MiruStreamEvent(
        @JsonProperty("tenantId") byte[] tenantId,
        @JsonProperty("time") long time,
        @JsonProperty("streamName") String streamName,
        @JsonProperty("type") String type,
        @JsonProperty("streamId") byte[] streamId,
        @JsonProperty("filter") MiruFilter filter) {

        this.tenantId = new MiruTenantId(tenantId);
        this.time = time;
        this.streamName = streamName;
        this.type = type;
        this.streamId = new MiruStreamId(streamId);
        this.filter = filter;
    }

    @JsonGetter("tenantId")
    public byte[] getTenantIdAsBytes() {
        return tenantId.getBytes();
    }

    @JsonGetter("streamId")
    public byte[] getStreamIdAsBytes() {
        return streamId.getBytes();
    }

    @Override
    public String toString() {
        return "MiruStreamEvent{" +
            "tenantId=" + tenantId +
            ", time=" + time +
            ", streamName='" + streamName + '\'' +
            ", type='" + type + '\'' +
            ", streamId=" + streamId +
            ", filter=" + filter +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MiruStreamEvent that = (MiruStreamEvent) o;

        if (time != that.time) {
            return false;
        }
        if (tenantId != null ? !tenantId.equals(that.tenantId) : that.tenantId != null) {
            return false;
        }
        if (streamName != null ? !streamName.equals(that.streamName) : that.streamName != null) {
            return false;
        }
        if (type != null ? !type.equals(that.type) : that.type != null) {
            return false;
        }
        if (streamId != null ? !streamId.equals(that.streamId) : that.streamId != null) {
            return false;
        }
        return filter != null ? filter.equals(that.filter) : that.filter == null;

    }

    @Override
    public int hashCode() {
        int result = tenantId != null ? tenantId.hashCode() : 0;
        result = 31 * result + (int) (time ^ (time >>> 32));
        result = 31 * result + (streamName != null ? streamName.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (streamId != null ? streamId.hashCode() : 0);
        result = 31 * result + (filter != null ? filter.hashCode() : 0);
        return result;
    }
}
