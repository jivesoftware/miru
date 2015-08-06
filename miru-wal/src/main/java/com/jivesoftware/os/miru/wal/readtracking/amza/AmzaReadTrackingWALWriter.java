package com.jivesoftware.os.miru.wal.readtracking.amza;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.JacksonJsonObjectTypeMarshaller;
import com.jivesoftware.os.miru.wal.AmzaWALUtil;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class AmzaReadTrackingWALWriter implements MiruReadTrackingWALWriter {

    private final AmzaWALUtil amzaWALUtil;
    private final int replicateRequireNReplicas;
    private final long replicateTimeoutMillis;
    private final Function<MiruPartitionedActivity, byte[]> readTrackingWALKeyFunction;
    private final Function<MiruPartitionedActivity, byte[]> activitySerializerFunction;

    public AmzaReadTrackingWALWriter(AmzaWALUtil amzaWALUtil,
        int replicateRequireNReplicas,
        long replicateTimeoutMillis,
        ObjectMapper mapper) {
        this.amzaWALUtil = amzaWALUtil;
        this.replicateRequireNReplicas = replicateRequireNReplicas;
        this.replicateTimeoutMillis = replicateTimeoutMillis;

        JacksonJsonObjectTypeMarshaller<MiruPartitionedActivity> partitionedActivityMarshaller =
            new JacksonJsonObjectTypeMarshaller<>(MiruPartitionedActivity.class, mapper);
        this.readTrackingWALKeyFunction = (partitionedActivity) -> FilerIO.longBytes(partitionedActivity.timestamp);
        this.activitySerializerFunction = partitionedActivity -> {
            try {
                return partitionedActivityMarshaller.toBytes(partitionedActivity);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public void write(MiruTenantId tenantId, MiruStreamId streamId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        amzaWALUtil.getReadTrackingClient(tenantId).commit(streamId.getBytes(),
            (highwaters, txKeyValueStream) -> {
                for (MiruPartitionedActivity activity : partitionedActivities) {
                    byte[] key = readTrackingWALKeyFunction.apply(activity);
                    byte[] value = activitySerializerFunction.apply(activity);
                    if (!txKeyValueStream.row(-1, key, value, System.currentTimeMillis(), false)) {
                        return false;
                    }
                }
                return true;
            },
            replicateRequireNReplicas,
            replicateTimeoutMillis,
            TimeUnit.MILLISECONDS);
    }
}
