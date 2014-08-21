package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.property.MiruPropertyName;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.service.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.service.query.base.ExecuteMiruFilter;
import com.jivesoftware.os.miru.service.stream.MiruQueryStream;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader.StreamReadTrackingSipWAL;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader.StreamReadTrackingWAL;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/** @author jonathan */
public class MiruJustInTimeBackfillerizer<BM> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();


    private final MiruHost localHost;
    private final MiruBitmaps<BM> bitmaps;
    private final ExecutorService backfillExecutor;

    public MiruJustInTimeBackfillerizer(MiruHost localHost, MiruBitmaps<BM> bitmaps, ExecutorService backfillExecutor) {
        this.localHost = localHost;
        this.bitmaps = bitmaps;
        this.backfillExecutor = backfillExecutor;
    }

    public void backfill(final MiruQueryStream stream, final MiruFilter streamFilter, final MiruTenantId tenantId,
        final MiruPartitionId partitionId, final MiruStreamId streamId) throws Exception {

        // backfill in another thread to guard WAL interface from solver cancellation/interruption
        Future<?> future = backfillExecutor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {

                    synchronized (stream.streamLocks.lock(streamId)) {
                        int lastActivityIndex = stream.inboxIndex.getLastActivityIndex(streamId);
                        int lastId = Math.min(stream.timeIndex.lastId(), stream.activityIndex.lastId());
                        BM answer = new ExecuteMiruFilter<BM>(bitmaps, stream.schema, stream.fieldIndex, stream.executorService,
                            streamFilter, Optional.<BM>absent(), lastActivityIndex).call();

                        MiruInvertedIndexAppender inbox = stream.inboxIndex.getAppender(streamId);
                        MiruInvertedIndexAppender unread = stream.unreadTrackingIndex.getAppender(streamId);
                        if (log.isDebugEnabled()) {
                            log.debug("before:\n  host={}\n  streamId={}\n  inbox={}\n  unread={}\n  last={}",
                                localHost, streamId.getBytes(), stream.inboxIndex.getInbox(streamId), stream.unreadTrackingIndex.getUnread(streamId),
                                lastActivityIndex);
                        }

                        MiruIBA streamIdAsIBA = new MiruIBA(streamId.getBytes());

                        long oldestBackfilledEventId = Long.MAX_VALUE;
                        //TODO more efficient way to merge answer into inbox and unread
                        MiruIntIterator intIterator = bitmaps.intIterator(answer);
                        List<Integer> inboxIds = Lists.newLinkedList();
                        List<Integer> unreadIds = Lists.newLinkedList();
                        while (intIterator.hasNext()) {
                            int i = intIterator.next();
                            if (i > lastActivityIndex && i <= lastId) {
                                MiruActivity miruActivity = stream.activityIndex.get(i);
                                if (miruActivity == null) {
                                    log.warn("Missing activity at index {}, timeIndex={}, activityIndex={}",
                                        i, stream.timeIndex.lastId(), stream.activityIndex.lastId());
                                    continue;
                                }
                                oldestBackfilledEventId = Math.min(oldestBackfilledEventId, miruActivity.time);

                                inboxIds.add(i);

                                MiruIBA[] readStreamIds = miruActivity.propsValues.get(MiruPropertyName.READ_STREAMIDS.getPropertyName());
                                if (readStreamIds == null || !Arrays.asList(readStreamIds).contains(streamIdAsIBA)) {
                                    unreadIds.add(i);
                                }
                            }
                        }
                        inbox.appendAndExtend(inboxIds, lastId);
                        unread.appendAndExtend(unreadIds, lastId);

                        stream.inboxIndex.setLastActivityIndex(streamId, lastId);

                        if (log.isDebugEnabled()) {
                            log.debug("after:\n  host={}\n  streamId={}\n  inbox={}\n  unread={}\n  last={}",
                                localHost, streamId.getBytes(), stream.inboxIndex.getInbox(streamId), stream.unreadTrackingIndex.getUnread(streamId),
                                lastActivityIndex);
                        }

                        sipAndApplyReadTracking(stream, tenantId, partitionId, streamId, lastId, oldestBackfilledEventId);
                    }

                } catch (Exception e) {
                    log.error("Backfillerizer failed", e);
                    throw new RuntimeException("Backfillerizer failed");
                }
                return null;
            }
        });

        // if this is interrupted, the backfill will still complete
        future.get();
    }

    private void sipAndApplyReadTracking(final MiruQueryStream stream, MiruTenantId tenantId, MiruPartitionId partitionId, MiruStreamId streamId,
        final int lastActivityIndex, long oldestBackfilledEventId) throws Exception {

        final AtomicLong minimumEventId = new AtomicLong(Long.MAX_VALUE);
        final AtomicLong newSipTimestamp = new AtomicLong(0);

        // First find the oldest eventId from our sip WAL
        long afterTimestamp = stream.readTrackingWALReader.getSipTimestamp(tenantId, partitionId, streamId);
        stream.readTrackingWALReader.streamSip(tenantId, streamId, afterTimestamp, new StreamReadTrackingSipWAL() {
            @Override
            public boolean stream(long eventId, long timestamp) throws Exception {
                long min = Math.min(minimumEventId.get(), eventId);
                minimumEventId.set(min);
                newSipTimestamp.set(timestamp);

                return true;
            }
        });

        // Take either the oldest backfilled time or the oldest readtracking sip time
        minimumEventId.set(Math.min(minimumEventId.get(), oldestBackfilledEventId));

        // If we aren't using the sip wal eventId, don't use the sip wal timestamp
        if (minimumEventId.get() == oldestBackfilledEventId) {
            newSipTimestamp.set(0);
        }

        // Take the oldest eventId from the sip WAL, grab all read/unread events from that time and apply them
        stream.readTrackingWALReader.stream(tenantId, streamId, minimumEventId.get(), new StreamReadTrackingWAL() {
            @Override
            public boolean stream(long eventId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception {
                MiruReadEvent readEvent = partitionedActivity.readEvent.get();
                MiruStreamId streamId = readEvent.streamId;
                MiruFilter filter = readEvent.filter;

                if (partitionedActivity.type == MiruPartitionedActivity.Type.READ) {
                    stream.readTrackStream.read(streamId, filter, lastActivityIndex, readEvent.time);
                } else if (partitionedActivity.type == MiruPartitionedActivity.Type.UNREAD) {
                    stream.readTrackStream.unread(streamId, filter, lastActivityIndex, readEvent.time);
                } else if (partitionedActivity.type == MiruPartitionedActivity.Type.MARK_ALL_READ) {
                    stream.readTrackStream.markAllRead(streamId, readEvent.time);
                }

                return true;
            }
        });

        // Update stream sip time if we used the read tracking sip WAL
        if (newSipTimestamp.get() != 0) {
            stream.readTrackingWALReader.setSipTimestamp(tenantId, partitionId, streamId, newSipTimestamp.get());
        }

    }
}
