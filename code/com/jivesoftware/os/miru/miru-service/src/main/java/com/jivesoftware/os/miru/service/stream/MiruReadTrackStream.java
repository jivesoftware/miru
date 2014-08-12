package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Optional;
import com.googlecode.javaewah.BitmapStorage;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.service.index.MiruFields;
import com.jivesoftware.os.miru.service.index.MiruTimeIndex;
import com.jivesoftware.os.miru.service.index.MiruUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.query.base.ExecuteMiruFilter;
import com.jivesoftware.os.miru.service.schema.MiruSchema;
import com.jivesoftware.os.miru.service.stream.factory.MiruFilterUtils;
import java.util.concurrent.ExecutorService;

/** @author jonathan */
public class MiruReadTrackStream {

    private final MiruFilterUtils utils;
    private final MiruSchema schema;
    private final MiruFields fieldIndex;
    private final MiruTimeIndex timeIndex;
    private final MiruUnreadTrackingIndex unreadTrackingIndex;
    private final ExecutorService executorService;
    private final int bitsetBufferSize;
    private final StripingLocksProvider<MiruStreamId> streamLocks;

    public MiruReadTrackStream(
        MiruFilterUtils utils,
        MiruSchema schema,
        MiruFields fieldIndex,
        MiruTimeIndex timeIndex,
        MiruUnreadTrackingIndex unreadTrackingIndex,
        ExecutorService executorService,
        int bitsetBufferSize,
        StripingLocksProvider<MiruStreamId> streamLocks) {
        this.utils = utils;
        this.schema = schema;
        this.fieldIndex = fieldIndex;
        this.timeIndex = timeIndex;
        this.unreadTrackingIndex = unreadTrackingIndex;
        this.executorService = executorService;
        this.bitsetBufferSize = bitsetBufferSize;
        this.streamLocks = streamLocks;
    }

    public void read(MiruStreamId streamId, MiruFilter filter, int lastActivityIndex, long lastActivityTimestamp) throws Exception {
        ExecuteMiruFilter executeMiruFilter = new ExecuteMiruFilter(schema, fieldIndex, executorService, filter, Optional.<BitmapStorage>absent(), -1,
            bitsetBufferSize);
        EWAHCompressedBitmap indexMask = utils.buildIndexMask(lastActivityIndex, Optional.<EWAHCompressedBitmap>absent());
        EWAHCompressedBitmap timeMask = utils.buildTimeRangeMask(timeIndex, 0L, lastActivityTimestamp);
        synchronized (streamLocks.lock(streamId)) {
            unreadTrackingIndex.applyRead(streamId, executeMiruFilter.call().and(indexMask).and(timeMask));
        }
    }

    public void unread(MiruStreamId streamId, MiruFilter filter, int lastActivityIndex, long lastActivityTimestamp) throws Exception {
        ExecuteMiruFilter executeMiruFilter = new ExecuteMiruFilter(schema, fieldIndex, executorService, filter, Optional.<BitmapStorage>absent(), -1,
            bitsetBufferSize);
        EWAHCompressedBitmap indexMask = utils.buildIndexMask(lastActivityIndex, Optional.<EWAHCompressedBitmap>absent());
        EWAHCompressedBitmap timeMask = utils.buildTimeRangeMask(timeIndex, 0L, lastActivityTimestamp);
        synchronized (streamLocks.lock(streamId)) {
            unreadTrackingIndex.applyUnread(streamId, executeMiruFilter.call().and(indexMask).and(timeMask));
        }
    }

    public void markAllRead(MiruStreamId streamId, long timestamp) throws Exception {
        EWAHCompressedBitmap timeMask = utils.buildTimeRangeMask(timeIndex, 0L, timestamp);
        synchronized (streamLocks.lock(streamId)) {
            unreadTrackingIndex.applyRead(streamId, timeMask);
        }
    }
}
