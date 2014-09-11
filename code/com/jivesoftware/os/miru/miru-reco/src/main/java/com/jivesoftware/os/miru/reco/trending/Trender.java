package com.jivesoftware.os.miru.reco.trending;

/**
 * @param <T> type for time id typically a Long
 * @param <G> type for trend return value typically the same as V
 * @author jonathan
 */
public interface Trender<T, G> {

    /**
     *
     * @param time
     * @throws Exception
     */
    public void add(T time, double amount) throws Exception;

    /**
     * @return
     */
    public T getMostRecentTimestamp();

    /**
     * used for testing primarily
     */
    public double[] getRawSignal();

    /**
     *
     * @param time
     * @return
     * @throws Exception
     */
    public G getTrend(T time) throws Exception;

    /**
     * @param time if null use most recent timestamp used during normalization
     */
    public G getMaxTrend(T time) throws Exception;

    /**
     *
     * @return
     * @throws Exception
     */
    public byte[] toBytes() throws Exception;

    /**
     *
     * @param t
     * @return
     * @throws Exception
     */
    public byte[] tToBytes(T t) throws Exception;

    /**
     *
     * @param g
     * @return
     * @throws Exception
     */
    public byte[] gToBytes(G g) throws Exception;

    /**
     *
     * @param bytes
     * @return
     * @throws Exception
     */
    public T bytesToT(byte[] bytes) throws Exception;

    /**
     *
     * @param bytes
     * @return
     * @throws Exception
     */
    public G bytesToG(byte[] bytes) throws Exception;

    /**
     * @param time if null use most recent timestamp
     */
    public double getRank(T time) throws Exception;

    /**
     *
     * @return
     */
    public T getCurrentT();

    /**
     *
     * @return
     */
    long[] getBucketsT();

    /**
     *
     * @param trend
     */
    void merge(Trender<T, G> trend) throws Exception;
}
