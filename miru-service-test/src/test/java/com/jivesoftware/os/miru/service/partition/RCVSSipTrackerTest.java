package com.jivesoftware.os.miru.service.partition;

import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.activity.TimeAndVersion;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class RCVSSipTrackerTest {

    private final int maxSipReplaySize = 100;
    private final long maxSipClockSkew = TimeUnit.SECONDS.toMillis(10);
    private final MiruTenantId tenantId = new MiruTenantId("test".getBytes());
    private final MiruPartitionId partitionId = MiruPartitionId.of(0);
    private final AtomicLong clockTimestamp = new AtomicLong();
    private final MiruPartitionedActivityFactory factory = new MiruPartitionedActivityFactory(clockTimestamp::get);

    @Test
    public void testSuggestTimestamp_empty() throws Exception {
        RCVSSipTracker sipTracker = new RCVSSipTracker(maxSipReplaySize, maxSipClockSkew, Sets.<TimeAndVersion>newHashSet());
        long initialTimestamp = System.currentTimeMillis();
        assertEquals(sipTracker.suggest(new RCVSSipCursor(MiruPartitionedActivity.Type.ACTIVITY.getSort(), initialTimestamp, 0, false), null),
            new RCVSSipCursor(MiruPartitionedActivity.Type.ACTIVITY.getSort(), initialTimestamp, 0, false));
    }

    @Test
    public void testSuggestTimestamp_fewerTimes_distant() throws Exception {
        long initialTimestamp = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        RCVSSipTracker sipTracker = new RCVSSipTracker(maxSipReplaySize, maxSipClockSkew, Sets.<TimeAndVersion>newHashSet());

        int numTimestamps = maxSipReplaySize / 2;
        final long firstTimestamp = System.currentTimeMillis() - 3 * maxSipClockSkew * numTimestamps;
        for (int i = 0; i < numTimestamps; i++) {
            sipTracker.track(buildPartitionedActivity(firstTimestamp + i * 2 * maxSipClockSkew, 0));
        }

        assertEquals(sipTracker.suggest(new RCVSSipCursor(MiruPartitionedActivity.Type.ACTIVITY.getSort(), initialTimestamp, 0, false), null),
            new RCVSSipCursor(MiruPartitionedActivity.Type.ACTIVITY.getSort(), firstTimestamp, 0, false));
    }

    @Test
    public void testSuggestTimestamp_fewerTimes_recent() throws Exception {
        long initialTimestamp = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        RCVSSipTracker sipTracker = new RCVSSipTracker(maxSipReplaySize, maxSipClockSkew, Sets.<TimeAndVersion>newHashSet());

        int numTimestamps = maxSipReplaySize / 2;
        final long firstTimestamp = System.currentTimeMillis() - numTimestamps;
        long lastTimestamp = 0;
        for (int i = 0; i < numTimestamps; i++) {
            lastTimestamp = firstTimestamp + i;
            sipTracker.track(buildPartitionedActivity(lastTimestamp, 0));
        }

        assertEquals(sipTracker.suggest(new RCVSSipCursor(MiruPartitionedActivity.Type.ACTIVITY.getSort(), initialTimestamp, 0, false), null),
            new RCVSSipCursor(MiruPartitionedActivity.Type.ACTIVITY.getSort(), lastTimestamp - maxSipClockSkew, 0, false));
    }

    private MiruPartitionedActivity buildPartitionedActivity(long clockTimestamp, long activityTimestamp) {
        this.clockTimestamp.set(clockTimestamp);
        return factory.activity(0, partitionId, 0,
            new MiruActivity(tenantId, activityTimestamp, 0, false, new String[0], Collections.emptyMap(), Collections.emptyMap()));
    }

    @Test
    public void testSuggestTimestamp_moreTimes_distant() throws Exception {
        long initialTimestamp = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        RCVSSipTracker sipTracker = new RCVSSipTracker(maxSipReplaySize, maxSipClockSkew, Sets.<TimeAndVersion>newHashSet());

        int numTimestamps = maxSipReplaySize * 2;
        final long firstTimestamp = System.currentTimeMillis() - 3 * maxSipClockSkew * numTimestamps;
        long lastTimestamp = 0;
        for (int i = 0; i < numTimestamps; i++) {
            lastTimestamp = firstTimestamp + i * 2 * maxSipClockSkew;
            sipTracker.track(buildPartitionedActivity(lastTimestamp, 0));
        }

        assertEquals(sipTracker.suggest(new RCVSSipCursor(MiruPartitionedActivity.Type.ACTIVITY.getSort(), initialTimestamp, 0, false), null),
            new RCVSSipCursor(MiruPartitionedActivity.Type.ACTIVITY.getSort(), lastTimestamp - maxSipClockSkew, 0, false));
    }

    @Test
    public void testSuggestTimestamp_moreTimes_recent() throws Exception {
        long initialTimestamp = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        RCVSSipTracker sipTracker = new RCVSSipTracker(maxSipReplaySize, maxSipClockSkew, Sets.<TimeAndVersion>newHashSet());

        int numTimestamps = maxSipReplaySize * 2;
        final long firstTimestamp = System.currentTimeMillis() - numTimestamps;
        long lastTimestamp = 0;
        for (int i = 0; i < numTimestamps; i++) {
            lastTimestamp = firstTimestamp + i;
            sipTracker.track(buildPartitionedActivity(lastTimestamp, 0));
        }

        final long oldestReplayTimestamp = lastTimestamp - maxSipReplaySize + 1;
        assertEquals(sipTracker.suggest(new RCVSSipCursor(MiruPartitionedActivity.Type.ACTIVITY.getSort(), initialTimestamp, 0, false), null),
            new RCVSSipCursor(MiruPartitionedActivity.Type.ACTIVITY.getSort(), oldestReplayTimestamp, 0, false));
    }

    @Test
    public void testWasSeenLastSip() throws Exception {
        long baseTime = System.currentTimeMillis();
        RCVSSipTracker lastSipTracker = new RCVSSipTracker(maxSipReplaySize, maxSipClockSkew, Sets.<TimeAndVersion>newHashSet());
        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) {
                lastSipTracker.addSeenThisSip(new TimeAndVersion(baseTime + i, i));
            }
        }

        MiruSipTracker sipTracker = new RCVSSipTracker(maxSipReplaySize, maxSipClockSkew, lastSipTracker.getSeenThisSip());
        for (int i = 0; i < 10; i++) {
            assertEquals(sipTracker.wasSeenLastSip(new TimeAndVersion(baseTime + i, i)), i % 2 == 0);
        }
    }
}
