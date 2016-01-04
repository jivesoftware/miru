package com.jivesoftware.os.miru.service.partition;

import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

public class MiruMergeChitsTest {

    @Test(enabled = false, description = "Tests convergence behavior")
    public void testMerge() throws Exception {

        final AtomicLong numberOfChitsRemaining = new AtomicLong(500_000);
        final MiruMergeChits mergeChits = new MiruMergeChits("test", numberOfChitsRemaining, 500_000, -1);

        ScheduledExecutorService scheduledInfo = Executors.newScheduledThreadPool(1);
        ScheduledExecutorService scheduledMergers = Executors.newScheduledThreadPool(8);
        List<Future<?>> futures = Lists.newArrayList();

        futures.add(scheduledInfo.scheduleWithFixedDelay(() -> System.out.println("free=" + mergeChits.remaining()), 1_000, 1_000, TimeUnit.MILLISECONDS));

        int[][] callersAndRates = new int[][] {
            { 1024, 1 },
            { 512, 2 },
            { 256, 4 },
            { 128, 8 },
            { 64, 16 },
            { 32, 32 },
            { 16, 64 },
            { 8, 128 },
            { 4, 256 }, };

        for (int i = 0; i < callersAndRates.length; i++) {
            final int callers = callersAndRates[i][0];
            final int rate = callersAndRates[i][1];
            for (int j = 0; j < callers; j++) {
                final AtomicLong lastTake = new AtomicLong(System.currentTimeMillis());
                final AtomicLong lastMerge = new AtomicLong(System.currentTimeMillis());
                final MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId(FilerIO.longBytes(i)),
                    MiruPartitionId.of(j),
                    new MiruHost("localhost:1234"));
                futures.add(scheduledMergers.scheduleWithFixedDelay(() -> {
                    try {
                        long currentTime = System.currentTimeMillis();
                        long elapsed = currentTime - lastMerge.get();
                        long count = (long) ((double) rate * (double) (currentTime - lastTake.getAndSet(currentTime)) / 1_000d);
                        if (mergeChits.take(coord, count)) {
                            notifyMerged(elapsed);
                            mergeChits.refundAll(coord);
                            lastMerge.set(currentTime);
                        }
                    } catch (Throwable t) {
                        System.out.println("Caller died");
                        t.printStackTrace();
                    }
                }, 1_000, 1_000, TimeUnit.MILLISECONDS));
            }
        }

        Thread.sleep(TimeUnit.HOURS.toMillis(10));

        scheduledInfo.shutdownNow();
        scheduledMergers.shutdownNow();
        scheduledInfo.awaitTermination(30, TimeUnit.SECONDS);
        scheduledMergers.awaitTermination(30, TimeUnit.SECONDS);
    }

    private final AtomicLong mergeCount = new AtomicLong(0);
    private final AtomicLong elapsedSum = new AtomicLong(0);

    private void notifyMerged(long elapsed) {
        elapsedSum.addAndGet(elapsed);
        long merged = mergeCount.incrementAndGet();
        if (merged % 10 == 0) {
            System.out.println("merged=" + merged + ", elapsed=" + (long) ((double) elapsedSum.getAndSet(0) / 10d));
        }
    }
}
