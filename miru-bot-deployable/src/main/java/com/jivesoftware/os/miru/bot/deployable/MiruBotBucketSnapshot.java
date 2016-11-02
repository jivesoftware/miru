package com.jivesoftware.os.miru.bot.deployable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

class MiruBotBucketSnapshot {

    String schemaName;
    String tenantId;
    long totalActivitiesGenerated;
    long fieldsValuesCount;
    MiruBotBucketSnapshotFields fieldsValuesCountStatus;
    String fieldsValuesFailed;

    static class MiruBotBucketSnapshotFields {

        long unknown;
        long written;
        long read_fail;
        long read_success;

        @JsonCreator
        MiruBotBucketSnapshotFields(
                @JsonProperty("unknown") long unknown,
                @JsonProperty("written") long written,
                @JsonProperty("read_fail") long read_fail,
                @JsonProperty("read_success") long read_success) {
            this.unknown = unknown;
            this.written = written;
            this.read_fail = read_fail;
            this.read_success = read_success;
        }

        public String toString() {
            return "MiruBotBucketSnapshotFields{" +
                    "unknown=" + unknown +
                    ", written=" + written +
                    ", read_fail=" + read_fail +
                    ", read_success=" + read_success +
                    '}';
        }
    }

    @JsonCreator
    MiruBotBucketSnapshot(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tenantId") String tenantId,
            @JsonProperty("totalActivitiesGenerated") long totalActivitiesGenerated,
            @JsonProperty("fieldsValuesCount") long fieldsValuesCount,
            @JsonProperty("fieldsValuesCountStatus") MiruBotBucketSnapshotFields fieldsValuesCountStatus,
            @JsonProperty("fieldsValuesFailed") String fieldsValuesFailed) {
        this.schemaName = schemaName;
        this.tenantId = tenantId;
        this.totalActivitiesGenerated = totalActivitiesGenerated;
        this.fieldsValuesCount = fieldsValuesCount;
        this.fieldsValuesCountStatus = fieldsValuesCountStatus;
        this.fieldsValuesFailed = fieldsValuesFailed;
    }

    public String toString() {
        return "MiruBotBucketSnapshot{" +
                "schemaName='" + schemaName + '\'' +
                ", tenantId=" + tenantId +
                ", totalActivitiesGenerated=" + totalActivitiesGenerated +
                ", fieldsValuesCount=" + fieldsValuesCount +
                ", fieldsValuesFailedCount=" + fieldsValuesCountStatus +
                ", fieldsValuesFailed='" + fieldsValuesFailed + '\'' +
                '}';
    }

}
