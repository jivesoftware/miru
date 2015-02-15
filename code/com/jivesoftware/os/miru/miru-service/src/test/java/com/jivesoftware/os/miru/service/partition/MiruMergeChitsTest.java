package com.jivesoftware.os.miru.service.partition;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

public class MiruMergeChitsTest {

    @Test(enabled = false, description = "Tests convergence behavior")
    public void testMerge() throws Exception {

        final MiruMergeChits mergeChits = new MiruMergeChits(2_000_000, 0.5, 1_000);

        ScheduledExecutorService scheduledInfo = Executors.newScheduledThreadPool(1);
        ScheduledExecutorService scheduledMergers = Executors.newScheduledThreadPool(8);
        List<Future<?>> futures = Lists.newArrayList();

        futures.add(scheduledInfo.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                System.out.println("free=" + mergeChits.numberOfChitsRemaining() + ", maxElapse=" + mergeChits.maxElapsedWithoutMergeInMillis());
            }
        }, 1_000, 1_000, TimeUnit.MILLISECONDS));

        final Semaphore mergeLock = new Semaphore(3);

        int[][] callersAndRates = new int[][] {
            { 1024, 1 },
            { 512,  2 },
            { 256,  4 },
            { 128,  8 },
            { 64,   16 },
            { 32,   32 },
            { 16,   64 },
            { 8,    128 },
            { 4,    256 },
        };

        for (int i = 0; i < callersAndRates.length; i++) {
            final int callers = callersAndRates[i][0];
            final int rate = callersAndRates[i][1];
            for (int j = 0; j < callers; j++) {
                final AtomicLong taken = new AtomicLong(0);
                final AtomicLong lastTake = new AtomicLong(System.currentTimeMillis());
                final AtomicLong lastMerge = new AtomicLong(System.currentTimeMillis());
                futures.add(scheduledMergers.scheduleWithFixedDelay(new Runnable() {
                    @Override
                    public void run() {
                        long currentTime = System.currentTimeMillis();
                        long elapsed = currentTime - lastMerge.get();
                        long count = (long) ((double) rate * (double) (currentTime - lastTake.getAndSet(currentTime)) / 1_000d);
                        mergeChits.take(count);
                        long have = taken.addAndGet(count);
                        if (mergeChits.merge(have, elapsed)) {
                            notifyMerged(elapsed);
                            mergeChits.refund(taken.getAndSet(0));
                            lastMerge.set(currentTime);
                            try {
                                mergeLock.acquire();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            } finally {
                                mergeLock.release();
                            }
                        }
                    }
                }, 1_000, 1_000, TimeUnit.MILLISECONDS));
                Thread.sleep(10);
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