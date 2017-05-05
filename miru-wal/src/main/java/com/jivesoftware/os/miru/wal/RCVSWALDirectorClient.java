package com.jivesoftware.os.miru.wal;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import com.jivesoftware.os.miru.api.activity.TimeAndVersion;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruVersionedActivityLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.miru.api.wal.RCVSCursor;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 */
public class RCVSWALDirectorClient implements MiruWALClient<RCVSCursor, RCVSSipCursor> {

    private final RCVSWALDirector director;

    public RCVSWALDirectorClient(RCVSWALDirector director) {
        this.director = director;
    }

    @Override
    public List<MiruTenantId> getAllTenantIds() throws Exception {
        return director.getAllTenantIds();
    }

    @Override
    public void writeActivity(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        director.writeActivity(tenantId, partitionId, partitionedActivities);
    }

    @Override
    public void writeReadTracking(MiruTenantId tenantId,
        List<MiruReadEvent> readEvents,
        Function<MiruReadEvent, MiruPartitionedActivity> transformer) throws Exception {

        ListMultimap<MiruStreamId, MiruPartitionedActivity> streamActivities = ArrayListMultimap.create();
        for (MiruReadEvent readEvent : readEvents) {
            MiruPartitionedActivity partitionedActivity = transformer.apply(readEvent);
            streamActivities.put(readEvent.streamId, partitionedActivity);
        }

        for (MiruStreamId streamId : streamActivities.keySet()) {
            List<MiruPartitionedActivity> partitionedActivities = streamActivities.get(streamId);
            director.writeReadTracking(tenantId, streamId, partitionedActivities);
        }
    }

    @Override
    public MiruPartitionId getLargestPartitionId(MiruTenantId tenantId) throws Exception {
        return director.getLargestPartitionId(tenantId);
    }

    @Override
    public WriterCursor getCursorForWriterId(MiruTenantId tenantId, MiruPartitionId partitionId, int writerId) throws Exception {
        return director.getCursorForWriterId(tenantId, partitionId, writerId);
    }

    @Override
    public MiruActivityWALStatus getActivityWALStatusForTenant(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return director.getActivityWALStatusForTenant(tenantId, partitionId);
    }

    @Override
    public long oldestActivityClockTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return director.oldestActivityClockTimestamp(tenantId, partitionId);
    }

    @Override
    public List<MiruVersionedActivityLookupEntry> getVersionedEntries(MiruTenantId tenantId, MiruPartitionId partitionId, Long[] timestamps) throws Exception {
        return director.getVersionedEntries(tenantId, partitionId, timestamps);
    }

    @Override
    public long getActivityCount(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return director.getActivityCount(tenantId, partitionId);
    }

    @Override
    public StreamBatch<MiruWALEntry, RCVSCursor> getActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        RCVSCursor cursor,
        int batchSize,
        long stopAtTimestamp,
        MutableLong bytesCount) throws Exception {
        return director.getActivity(tenantId, partitionId, cursor, batchSize, stopAtTimestamp, bytesCount);
    }

    @Override
    public StreamBatch<MiruWALEntry, RCVSSipCursor> sipActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        RCVSSipCursor cursor,
        Set<TimeAndVersion> lastSeen,
        int batchSize) throws Exception {
        return director.sipActivity(tenantId, partitionId, cursor, lastSeen, batchSize);
    }

    @Override
    public OldestReadResult<RCVSSipCursor> oldestReadEventId(MiruTenantId tenantId,
        MiruStreamId streamId,
        RCVSSipCursor cursor,
        boolean createIfAbsent) throws Exception {
        return director.oldestReadEventId(tenantId, streamId, cursor);
    }

    @Override
    public StreamBatch<MiruWALEntry, Long> scanRead(MiruTenantId tenantId,
        MiruStreamId streamId,
        long fromTimestamp,
        int batchSize,
        boolean createIfAbsent) throws Exception {
        return director.scanRead(tenantId, streamId, fromTimestamp, batchSize);
    }
}
