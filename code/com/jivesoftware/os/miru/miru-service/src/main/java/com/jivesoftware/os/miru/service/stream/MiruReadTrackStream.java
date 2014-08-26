package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.service.index.MiruFields;
import com.jivesoftware.os.miru.service.index.MiruTimeIndex;
import com.jivesoftware.os.miru.service.index.MiruUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.query.base.ExecuteMiruFilter;
import com.jivesoftware.os.miru.service.stream.factory.MiruFilterUtils;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

/** @author jonathan */
public class MiruReadTrackStream<BM> {

    private final MiruBitmaps<BM> bitmaps;
    private final MiruFilterUtils<BM> utils;
    private final MiruSchema schema;
    private final MiruFields<BM> fieldIndex;
    private final MiruTimeIndex timeIndex;
    private final MiruUnreadTrackingIndex <BM>unreadTrackingIndex;
    private final ExecutorService executorService;
    private final StripingLocksProvider<MiruStreamId> streamLocks;

    public MiruReadTrackStream(MiruBitmaps<BM> bitmaps,
        MiruFilterUtils<BM> utils,
        MiruSchema schema,
        MiruFields<BM> fieldIndex,
        MiruTimeIndex timeIndex,
        MiruUnreadTrackingIndex<BM> unreadTrackingIndex,
        ExecutorService executorService,
        StripingLocksProvider<MiruStreamId> streamLocks) {

        this.bitmaps = bitmaps;
        this.utils = utils;
        this.schema = schema;
        this.fieldIndex = fieldIndex;
        this.timeIndex = timeIndex;
        this.unreadTrackingIndex = unreadTrackingIndex;
        this.executorService = executorService;
        this.streamLocks = streamLocks;
    }

    public void read(MiruStreamId streamId, MiruFilter filter, int lastActivityIndex, long lastActivityTimestamp) throws Exception {
        ExecuteMiruFilter<BM> executeMiruFilter = new ExecuteMiruFilter(bitmaps, schema, fieldIndex, executorService, filter, Optional.<BM>absent(), -1);
        BM indexMask = bitmaps.buildIndexMask(lastActivityIndex, Optional.<BM>absent());
        BM timeMask = bitmaps.buildTimeRangeMask(timeIndex, 0L, lastActivityTimestamp);
        synchronized (streamLocks.lock(streamId)) {
            BM result = bitmaps.create();
            bitmaps.and(result, Arrays.asList(executeMiruFilter.call(), indexMask, timeMask));
            unreadTrackingIndex.applyRead(streamId, result);
        }
    }

    public void unread(MiruStreamId streamId, MiruFilter filter, int lastActivityIndex, long lastActivityTimestamp) throws Exception {
        ExecuteMiruFilter<BM> executeMiruFilter = new ExecuteMiruFilter<>(bitmaps, schema, fieldIndex, executorService, filter, Optional.<BM>absent(), -1);
        BM indexMask = bitmaps.buildIndexMask(lastActivityIndex, Optional.<BM>absent());
        BM timeMask = bitmaps.buildTimeRangeMask(timeIndex, 0L, lastActivityTimestamp);
        synchronized (streamLocks.lock(streamId)) {
            BM result = bitmaps.create();
            bitmaps.and(result, Arrays.asList(executeMiruFilter.call(), indexMask, timeMask));
            unreadTrackingIndex.applyUnread(streamId, result);
        }
    }

    public void markAllRead(MiruStreamId streamId, long timestamp) throws Exception {
        BM timeMask = bitmaps.buildTimeRangeMask(timeIndex, 0L, timestamp);
        synchronized (streamLocks.lock(streamId)) {
            unreadTrackingIndex.applyRead(streamId, timeMask);
        }
    }
}
