package com.jivesoftware.os.miru.reco.trending;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.ordered.id.IdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.apache.commons.math.stat.regression.SimpleRegression;

/**
 *
 */
public class SimpleRegressionTrend implements Trender<Long, GotSimpleRegressionTrend> {

    private static final MetricLogger logger = MetricLoggerFactory.getLogger();
    private static final IdPacker idPacker = new SnowflakeIdPacker();

    public static final int numberOfBuckets = 28;
    public static final int smoothingWidthInNumberOfBuckets = 8;
    public static final long durationPerBucket = idPacker.pack(TimeUnit.HOURS.toMillis(3), 0, 0);

    private final SparseCircularHitsBucketBuffer hitsBuffer;
    private final TrendRank<SimpleRegression> trendRanker;
    private final TrendRecency<SimpleRegressionTrend> recencyRanker;
    private int lookBack;

    public SimpleRegressionTrend() {
        this(numberOfBuckets, smoothingWidthInNumberOfBuckets, 0, durationPerBucket, new SlopeTrendRank(), new LinearTimeDecayTrendRecency());
    }

    public SimpleRegressionTrend(int size, int smoothingLookback, long utcOffset, long windowWidthMillis,
            TrendRank<SimpleRegression> trendRanker, TrendRecency<SimpleRegressionTrend> recencyRanker) {
        this.lookBack = smoothingLookback;
        this.trendRanker = trendRanker;
        this.recencyRanker = recencyRanker;
        hitsBuffer = new SparseCircularHitsBucketBuffer(size, utcOffset, windowWidthMillis);
    }

    @Override
    public void add(Long time, double amount) throws Exception {
        hitsBuffer.push(time, amount);
    }

    @Override
    public GotSimpleRegressionTrend getTrend(Long time) throws Exception {
        SimpleRegression r = getRegression(time);
        GotSimpleRegressionTrend t = new GotSimpleRegressionTrend();
        t.rank = getRank(time);
        t.recency = getRecency(time);
        t.intercept = r.getIntercept();
        t.interceptStdErr = r.getInterceptStdErr();
        t.slope = r.getSlope();
        t.slopeStdErr = r.getSlopeStdErr();
        t.rsquared = r.getRSquare();
        return t;
    }

    /**
     * get signal without moving cursor
     *
     * @return
     */
    @Override
    public double[] getRawSignal() {
        return hitsBuffer.rawSignal();
    }

    /**
     * get signal after moving cursor to time
     *
     * @param time
     * @return
     */
    public double[] getRawSignal(Long time) {
        if (time == null) {
            throw new IllegalArgumentException("Time cannot be null"); // todo ?? use assert
        }
        hitsBuffer.push(time, 0d); // move cursor to present
        return getRawSignal();
    }

    @Override
    public Long getMostRecentTimestamp() {
        return hitsBuffer.mostRecentTimestamp();
    }

    public long getDuration() {
        return hitsBuffer.duration();
    }

    public SimpleRegression getRegression(Long time) {
        if (time == null) {
            throw new IllegalArgumentException("Time cannot be null"); // todo ?? use assert
        }
        hitsBuffer.push(time, 0d); // move cursor to present
        SimpleRegression r = new SimpleRegression();
        double[] raw = hitsBuffer.rawSignal();
        // todo: have foregone smoothing altogether! re-eval if smoothing is needed / appropriate
        double[] smooth = raw; // with low hit counts this does more harm than good! MathFunctions.ema(raw, lookBack);
        int l = smooth.length;
        for (int i = 0; i < l; i++) {
            double s = (double) i / (double) (l - 1);
            r.addData(s, smooth[i]);
        }
        return r;
    }

    @Override
    public byte[] toBytes() throws Exception {
        byte[] hitsBufferBytes = hitsBuffer.toBytes();
        ByteBuffer bb = ByteBuffer.allocate(4 + hitsBufferBytes.length);
        bb.putInt(lookBack);
        bb.put(hitsBufferBytes);
        return bb.array();
    }

    @Override
    public void initWithBytes(byte[] bytes) throws Exception {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        lookBack = bb.getInt();
        byte[] hitsBufferBytes = new byte[bb.remaining()];
        bb.get(hitsBufferBytes);
        hitsBuffer.fromBytes(hitsBufferBytes);
    }

    @Override
    public byte[] tToBytes(Long t) throws Exception {
        if (t == null) {
            return null;
        }
        return longBytes(t, new byte[8], 0);
    }

    @Override
    public byte[] gToBytes(GotSimpleRegressionTrend g) throws Exception {
        return g.toBytes();
    }

    @Override
    public Long bytesToT(byte[] bytes) throws Exception {
        return bytesLong(bytes, 0);
    }

    private byte[] longBytes(long v, byte[] _bytes, int _offset) {
        _bytes[_offset + 0] = (byte) (v >>> 56);
        _bytes[_offset + 1] = (byte) (v >>> 48);
        _bytes[_offset + 2] = (byte) (v >>> 40);
        _bytes[_offset + 3] = (byte) (v >>> 32);
        _bytes[_offset + 4] = (byte) (v >>> 24);
        _bytes[_offset + 5] = (byte) (v >>> 16);
        _bytes[_offset + 6] = (byte) (v >>> 8);
        _bytes[_offset + 7] = (byte) v;
        return _bytes;
    }

    private long bytesLong(byte[] bytes, int _offset) {
        if (bytes == null) {
            return 0;
        }
        long v = 0;
        v |= (bytes[_offset + 0] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 1] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 2] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 3] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 4] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 5] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 6] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 7] & 0xFF);
        return v;
    }

    @Override
    public GotSimpleRegressionTrend bytesToG(byte[] bytes) throws Exception {
        GotSimpleRegressionTrend g = new GotSimpleRegressionTrend();
        g.fromBytes(bytes);
        return g;
    }

    /**
     * @return smaller is better
     * @throws java.lang.Exception
     */
    @Override
    public double getRank(Long time) throws Exception {
        if (time == null) {
            time = hitsBuffer.mostRecentTimestamp();
        }
        SimpleRegression r = getRegression(time);
        double rank = trendRanker.getRank(time, r);
        if (Double.isNaN(rank)) {
            rank = 0d;
            logger.warn("trendRanker=" + trendRanker + " returned NaN! Recoverd by using 0d!");
        }
        return rank;
    }

    /**
     * @return smaller is better
     * @throws java.lang.Exception
     */
    @Override
    public GotSimpleRegressionTrend getMaxTrend(Long time) throws Exception {
        SimpleRegression r = getMaxRegression(time);
        GotSimpleRegressionTrend t = new GotSimpleRegressionTrend();
        t.rank = getRank(time);
        t.recency = getRecency(time);
        t.intercept = r.getIntercept();
        t.interceptStdErr = r.getInterceptStdErr();
        t.slope = r.getSlope();
        t.slopeStdErr = r.getSlopeStdErr();
        t.rsquared = r.getRSquare();
        return t;
    }

    public double getMaxRank(Long time) throws Exception {
        if (time == null) {
            time = hitsBuffer.mostRecentTimestamp();
        }
        SimpleRegression r = getMaxRegression(time);
        double rank = trendRanker.getRank(time, r);
        if (Double.isNaN(rank)) {
            rank = 0d;
            logger.warn("trendRanker=" + trendRanker + " returned NaN! Recoverd by using 0d!");
        }
        return rank;
    }

    /**
     * Creates a best case regression from max to 0
     */
    private SimpleRegression getMaxRegression(Long time) {
        if (time == null) {
            throw new IllegalArgumentException("Time cannot be null"); // todo ?? use assert
        }
        hitsBuffer.push(time, 0d); // move cursor to present
        SimpleRegression r = new SimpleRegression();
        double[] raw = hitsBuffer.rawSignal();
        double max = 0;
        for (double v : raw) {
            if (max < v) {
                max = v;
            }
        }
        int l = raw.length;
        double gainPerStep = max / (double) l;
        double v = 0;
        for (int i = 0; i < l; i++) {
            double s = (double) i / (double) (l - 1);
            r.addData(s, v);
            v += gainPerStep;
        }
        return r;
    }

    public double getRecency(Long time) throws Exception {
        if (time == null) {
            throw new IllegalArgumentException("time cannot be null");
        }
        double rank = recencyRanker.getRecency(time, this);
        if (Double.isNaN(rank)) {
            rank = 0d;
            logger.warn("recencyRanker=" + recencyRanker + " returned NaN! Recoverd by using 0d!");
        }
        return rank;
    }

    @Override
    public Long getCurrentT() {
        return idPacker.pack(System.currentTimeMillis(), 0, 0);
    }

    @Override
    public long[] getBucketsT() {
        return hitsBuffer.bucketTimes();
    }

    @Override
    public void merge(Trender<Long, GotSimpleRegressionTrend> other) throws Exception {
        double[] rawSignal = other.getRawSignal();
        long[] times = other.getBucketsT();
        for (int i = 0; i < rawSignal.length; i++) {
            add(times[i], rawSignal[i]);
        }
    }

    @Override
    public String toString() {
        return "SimpleRegressionTrend{"
                + "hitsBuffer=" + hitsBuffer
                + ", lookBack=" + lookBack
                + '}';
    }
}
