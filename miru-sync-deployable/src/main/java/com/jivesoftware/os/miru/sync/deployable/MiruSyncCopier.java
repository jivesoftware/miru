package com.jivesoftware.os.miru.sync.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.client.aquarium.AmzaClientAquariumProvider;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.activity.TenantAndPartition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.sync.MiruSyncClient;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus.WriterCount;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.RoutingGroupType;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.StreamBatch;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.jivesoftware.os.miru.api.wal.MiruWALClient.RoutingGroupType.activity;
import static com.jivesoftware.os.miru.sync.deployable.MiruSyncSender.ProgressType.forward;
import static com.jivesoftware.os.miru.sync.deployable.MiruSyncSender.ProgressType.initial;
import static com.jivesoftware.os.miru.sync.deployable.MiruSyncSender.ProgressType.reverse;

/**
 *
 */
public class MiruSyncCopier<C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruWALClient<C, S> walClient;
    private final int batchSize;
    private final C defaultCursor;
    private final Class<C> cursorClass;

    public MiruSyncCopier(MiruWALClient<C, S> walClient, int batchSize, C defaultCursor, Class<C> cursorClass) {
        this.walClient = walClient;
        this.batchSize = batchSize;
        this.defaultCursor = defaultCursor;
        this.cursorClass = cursorClass;
    }

    public int copyLocal(MiruTenantId tenantId, MiruPartitionId fromPartitionId, MiruPartitionId toPartitionId, long fromTimestamp) throws Exception {
        C cursor = defaultCursor;

        int copied = 0;
        boolean started = false;
        while (true) {
            StreamBatch<MiruWALEntry, C> batch = walClient.getActivity(tenantId, fromPartitionId, cursor, batchSize, -1);
            if (batch.activities != null && !batch.activities.isEmpty()) {
                List<MiruPartitionedActivity> copyOfActivities = Lists.newArrayListWithCapacity(batch.activities.size());
                for (MiruWALEntry entry : batch.activities) {
                    MiruPartitionedActivity activity = entry.activity;
                    if (activity.type.isActivityType() && activity.timestamp > fromTimestamp) {
                        started = true;
                    }

                    if (started) {
                        // hacky use of fromJson()
                        copyOfActivities.add(MiruPartitionedActivity.fromJson(activity.type,
                            activity.writerId,
                            toPartitionId.getId(),
                            activity.tenantId.getBytes(),
                            activity.index,
                            activity.timestamp,
                            activity.clockTimestamp,
                            activity.activity.orNull(),
                            activity.readEvent.orNull()));
                    }
                }
                if (copyOfActivities.isEmpty()) {
                    break;
                } else {
                    walClient.writeActivity(tenantId, toPartitionId, copyOfActivities);
                    copied += copyOfActivities.size();
                }
            }
            cursor = batch.cursor;
        }
        return copied;
    }
}
