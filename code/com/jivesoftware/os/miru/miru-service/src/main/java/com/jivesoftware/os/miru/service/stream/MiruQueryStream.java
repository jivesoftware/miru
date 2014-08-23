package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.service.index.MiruActivityIndex;
import com.jivesoftware.os.miru.service.index.MiruFields;
import com.jivesoftware.os.miru.service.index.MiruInboxIndex;
import com.jivesoftware.os.miru.service.index.MiruRemovalIndex;
import com.jivesoftware.os.miru.service.index.MiruTimeIndex;
import com.jivesoftware.os.miru.service.index.MiruUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzIndex;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import java.util.concurrent.ExecutorService;

/**
 * @author jonathan
 */
public class MiruQueryStream<BM> {

    public final ExecutorService executorService;
    public final MiruSchema schema;
    public final MiruTimeIndex timeIndex;
    public final MiruActivityIndex activityIndex;
    public final MiruFields<BM> fieldIndex;
    public final MiruAuthzIndex<BM> authzIndex;
    public final MiruRemovalIndex<BM> removalIndex;
    public final MiruUnreadTrackingIndex<BM> unreadTrackingIndex;
    public final MiruInboxIndex<BM> inboxIndex;
    public final MiruReadTrackStream <BM>readTrackStream;
    public final MiruReadTrackingWALReader readTrackingWALReader;
    public final StripingLocksProvider<MiruStreamId> streamLocks;

    public MiruQueryStream(ExecutorService executorService,
        MiruSchema schema,
        MiruTimeIndex timeIndex,
        MiruActivityIndex activityIndex,
        MiruFields<BM> fieldIndex,
        MiruAuthzIndex<BM> authzIndex,
        MiruRemovalIndex<BM> removalIndex,
        MiruUnreadTrackingIndex<BM> unreadTrackingIndex,
        MiruInboxIndex<BM> inboxIndex,
        MiruReadTrackStream<BM> readTrackStream,
        MiruReadTrackingWALReader readTrackingWALReader,
        StripingLocksProvider<MiruStreamId> streamLocks) {
        this.executorService = executorService;
        this.schema = schema;
        this.timeIndex = timeIndex;
        this.activityIndex = activityIndex;
        this.fieldIndex = fieldIndex;
        this.authzIndex = authzIndex;
        this.removalIndex = removalIndex;
        this.unreadTrackingIndex = unreadTrackingIndex;
        this.inboxIndex = inboxIndex;
        this.readTrackStream = readTrackStream;
        this.readTrackingWALReader = readTrackingWALReader;
        this.streamLocks = streamLocks;
    }
}
