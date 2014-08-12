package com.jivesoftware.os.miru.service.partition;

import com.google.common.collect.Sets;
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
        assertEquals(sipTracker.suggestTimestamp(initialTimestamp), initialTimestamp);
    }

    @Test
    public void testSuggestTimestamp_fewerTimes_distant() throws Exception {
        long initialTimestamp = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        MiruSipTracker sipTracker = new MiruSipTracker(maxSipReplaySize, maxSipClockSkew, Sets.<TimeAndVersion>newHashSet());

        int numTimestamps = maxSipReplaySize / 2;
        final long firstTimestamp = System.currentTimeMillis() - 3 * maxSipClockSkew * numTimestamps;
        for (int i = 0; i < numTimestamps; i++) {
            sipTracker.put(firstTimestamp + i * 2 * maxSipClockSkew);
        }

        assertEquals(sipTracker.suggestTimestamp(initialTimestamp), firstTimestamp);
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
            sipTracker.put(lastTimestamp);
        }

        assertEquals(sipTracker.suggestTimestamp(initialTimestamp), lastTimestamp - maxSipClockSkew);
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
            sipTracker.put(lastTimestamp);
        }

        assertEquals(sipTracker.suggestTimestamp(initialTimestamp), lastTimestamp - maxSipClockSkew);
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
            sipTracker.put(lastTimestamp);
        }

        final long oldestReplayTimestamp = lastTimestamp - maxSipReplaySize + 1;
        assertEquals(sipTracker.suggestTimestamp(initialTimestamp), oldestReplayTimestamp + 1);
    }

}