package com.jivesoftware.os.miru.reco.trending;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Supports storing a hits waveform. You can add hit @ time in any order.
 * The Buffer will slide to accommodate hits.
 *
 * NOT thread safe any more, synchronize externally.
 *
 * @author jonathan
 */
public class SparseCircularHitsBucketBuffer {

    private static final MetricLogger logger = MetricLoggerFactory.getLogger();

    private long mostRecentTimeStamp = Long.MIN_VALUE;
    private long oldestBucketNumber = Long.MIN_VALUE;
    private long youngestBucketNumber;
    private long bucketEdgeMillis;
    private long bucketWidthMillis;
    private int cursor; // always points oldest bucket. cursor -1 is the newestBucket
    private int size;
    private double[] hits;

    public SparseCircularHitsBucketBuffer(int size, long bucketEdgeMillis, long bucketWidthMillis) {
        this.size = size;
        this.bucketEdgeMillis = bucketEdgeMillis; // supports timezones with use as a offest from UTC
        this.bucketWidthMillis = bucketWidthMillis;
        hits = new double[size];
    }

    public long mostRecentTimestamp() {
        return mostRecentTimeStamp;
    }

    public long duration() {
        return bucketWidthMillis * size;
    }

    public byte[] toBytes() throws Exception {
        ByteBuffer bb = ByteBuffer.allocate((8 * 5) + (4 * 2) + (8 * size));
        bb.putLong(mostRecentTimeStamp);
        bb.putLong(oldestBucketNumber);
        bb.putLong(youngestBucketNumber);
        bb.putLong(bucketEdgeMillis);
        bb.putLong(bucketWidthMillis);
        bb.putInt(cursor);
        bb.putInt(size);
        for (int i = 0; i < size; i++) {
            bb.putDouble(hits[i]);
        }
        return bb.array();
    }

    public void fromBytes(byte[] raw) throws Exception {
        ByteBuffer bb = ByteBuffer.wrap(raw);
        mostRecentTimeStamp = bb.getLong();
        oldestBucketNumber = bb.getLong();
        youngestBucketNumber = bb.getLong();
        bucketEdgeMillis = bb.getLong();
        bucketWidthMillis = bb.getLong();
        cursor = bb.getInt();
        size = bb.getInt();
        hits = new double[size];
        for (int i = 0; i < size; i++) {
            hits[i] = bb.getDouble();
        }
    }

    public void push(long time, double hit) {
        if (time > mostRecentTimeStamp) {
            mostRecentTimeStamp = time;
        }
        long absBucketNumber = absBucketNumber(time);
        if (oldestBucketNumber == Long.MIN_VALUE) {
            oldestBucketNumber = absBucketNumber - (size - 1);
            youngestBucketNumber = absBucketNumber;
        } else {
            if (absBucketNumber < oldestBucketNumber) {
                logger.warn("Moving backwards is unsupported so we will simply drop the hit on the floor!");
                return;
            }
            if (absBucketNumber > youngestBucketNumber) {
                // we need to slide the buffer to accommodate younger values
                long delta = absBucketNumber - youngestBucketNumber;
                for (int i = 0; i < delta; i++) {
                    hits[cursor] = 0; // zero out oldest
                    cursor = nextCursor(cursor, 1); // move cursor
                }
                oldestBucketNumber += delta;
                youngestBucketNumber = absBucketNumber;
            }
        }
        int delta = (int) (absBucketNumber - oldestBucketNumber);
        hits[nextCursor(cursor, delta)] += hit;

    }

    private long absBucketNumber(long time) {
        long absBucketNumber = time / bucketWidthMillis;
        long absNearestEdge = bucketWidthMillis * absBucketNumber;
        long remainder = time - (absNearestEdge);
        if (remainder < bucketEdgeMillis) {
            return absBucketNumber - 1;
        } else {
            return absBucketNumber;
        }
    }

    private int nextCursor(int cursor, int move) {
        cursor += move;
        if (cursor >= size) {
            cursor = cursor - size;
        }
        return cursor;
    }

    public double[] rawSignal() {
        double[] copy = new double[size];
        int c = cursor;
        for (int i = 0; i < size; i++) {
            copy[i] = hits[c];
            c = nextCursor(c, 1);
        }
        return copy;
    }

    public long[] bucketTimes() {
        long[] times = new long[size];
        long t = mostRecentTimeStamp - (size - 1) * bucketWidthMillis;
        for (int i = cursor, j = 0; j < size; i = (i + 1) % size, j++) {
            times[i] = t;
            t += bucketWidthMillis;
        }
        return times;
    }

    @Override
    public String toString() {
        return "SparseCircularHitsBucketBuffer{" +
            "mostRecentTimeStamp=" + mostRecentTimeStamp +
            ", oldestBucketNumber=" + oldestBucketNumber +
            ", youngestBucketNumber=" + youngestBucketNumber +
            ", bucketEdgeMillis=" + bucketEdgeMillis +
            ", bucketWidthMillis=" + bucketWidthMillis +
            ", cursor=" + cursor +
            ", size=" + size +
            ", hits=" + Arrays.toString(hits) +
            '}';
    }
}
