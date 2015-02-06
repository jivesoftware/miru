package com.jivesoftware.os.miru.service.partition;

import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader.Sip;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class MiruSipTrackerTest {

    private final int maxSipReplaySize = 100;
    private final long maxSipClockSkew = TimeUnit.SECONDS.toMillis(10);

    @Test
    public void testSuggestTimestamp_empty() throws Exception {
        MiruSipTracker sipTracker = new MiruSipTracker(maxSipReplaySize, maxSipClockSkew, Sets.<TimeAndVersion>newHashSet());
        long initialTimestamp = System.currentTimeMillis();
        assertEquals(sipTracker.suggest(new Sip(initialTimestamp, 0)), new Sip(initialTimestamp, 0));
    }

    @Test
    public void testSuggestTimestamp_fewerTimes_distant() throws Exception {
        long initialTimestamp = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        MiruSipTracker sipTracker = new MiruSipTracker(maxSipReplaySize, maxSipClockSkew, Sets.<TimeAndVersion>newHashSet());

        int numTimestamps = maxSipReplaySize / 2;
        final long firstTimestamp = System.currentTimeMillis() - 3 * maxSipClockSkew * numTimestamps;
        for (int i = 0; i < numTimestamps; i++) {
            sipTracker.put(firstTimestamp + i * 2 * maxSipClockSkew, 0);
        }

        assertEquals(sipTracker.suggest(new Sip(initialTimestamp, 0)), new Sip(firstTimestamp, 0));
    }

    @Test
    public void testSuggestTimestamp_fewerTimes_recent() throws Exception {
        long initialTimestamp = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        MiruSipTracker sipTracker = new MiruSipTracker(maxSipReplaySize, maxSipClockSkew, Sets.<TimeAndVersion>newHashSet());

        int numTimestamps = maxSipReplaySize / 2;
        final long firstTimestamp = System.currentTimeMillis() - numTimestamps;
        long lastTimestamp = 0;
        for (int i = 0; i < numTimestamps; i++) {
            lastTimestamp = firstTimestamp + i;
            sipTracker.put(lastTimestamp, 0);
        }

        assertEquals(sipTracker.suggest(new Sip(initialTimestamp, 0)), new Sip(lastTimestamp - maxSipClockSkew, 0));
    }

    @Test
    public void testSuggestTimestamp_moreTimes_distant() throws Exception {
        long initialTimestamp = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        MiruSipTracker sipTracker = new MiruSipTracker(maxSipReplaySize, maxSipClockSkew, Sets.<TimeAndVersion>newHashSet());

        int numTimestamps = maxSipReplaySize * 2;
        final long firstTimestamp = System.currentTimeMillis() - 3 * maxSipClockSkew * numTimestamps;
        long lastTimestamp = 0;
        for (int i = 0; i < numTimestamps; i++) {
            lastTimestamp = firstTimestamp + i * 2 * maxSipClockSkew;
            sipTracker.put(lastTimestamp, 0);
        }

        assertEquals(sipTracker.suggest(new Sip(initialTimestamp, 0)), new Sip(lastTimestamp - maxSipClockSkew, 0));
    }

    @Test
    public void testSuggestTimestamp_moreTimes_recent() throws Exception {
        long initialTimestamp = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        MiruSipTracker sipTracker = new MiruSipTracker(maxSipReplaySize, maxSipClockSkew, Sets.<TimeAndVersion>newHashSet());

        int numTimestamps = maxSipReplaySize * 2;
        final long firstTimestamp = System.currentTimeMillis() - numTimestamps;
        long lastTimestamp = 0;
        for (int i = 0; i < numTimestamps; i++) {
            lastTimestamp = firstTimestamp + i;
            sipTracker.put(lastTimestamp, 0);
        }

        final long oldestReplayTimestamp = lastTimestamp - maxSipReplaySize + 1;
        assertEquals(sipTracker.suggest(new Sip(initialTimestamp, 0)), new Sip(oldestReplayTimestamp, 0));
    }

    @Test
    public void testWasSeenLastSip() throws Exception {
        long baseTime = System.currentTimeMillis();
        MiruSipTracker lastSipTracker = new MiruSipTracker(maxSipReplaySize, maxSipClockSkew, Sets.<TimeAndVersion>newHashSet());
        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) {
                lastSipTracker.addSeenThisSip(new TimeAndVersion(baseTime + i, i));
            }
        }

        MiruSipTracker sipTracker = new MiruSipTracker(maxSipReplaySize, maxSipClockSkew, lastSipTracker.getSeenThisSip());
        for (int i = 0; i < 10; i++) {
            assertEquals(sipTracker.wasSeenLastSip(new TimeAndVersion(baseTime + i, i)), i % 2 == 0);
        }
    }
}
