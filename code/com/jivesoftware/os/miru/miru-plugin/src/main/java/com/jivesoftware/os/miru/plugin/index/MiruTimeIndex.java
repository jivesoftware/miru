/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.jivesoftware.os.miru.plugin.index;

/**
 * @author jonathan
 */
public interface MiruTimeIndex {

    boolean contains(long timestamp);

    /**
     * Returns the actual index of the given timestamp if it appears in the index, or else where it would have been.
     *
     * @param timestamp the activity timestamp
     * @return the actual index or nearest insertion point
     */
    int getClosestId(long timestamp);

    int getExactId(long timestamp);

    long getLargestTimestamp();

    long getSmallestTimestamp();

    long getTimestamp(int id);

    /**
     * Returns the smallest id satisfying the condition that {@link #getTimestamp(int)}
     * is greater than the requested timestamp (exclusive lower bound).
     *
     * @param timestamp the timestamp serving as the exclusive lower bound
     * @return the smallest id exclusive of the requested timestamp
     */
    int smallestExclusiveTimestampIndex(long timestamp);

    /**
     * Returns the largest id satisfying the condition that {@link #getTimestamp(int)}
     * is less than or equal to the requested timestamp (inclusive upper bound).
     *
     * @param timestamp the timestamp serving as the inclusive upper bound
     * @return the largest id inclusive of the requested timestamp
     */
    int largestInclusiveTimestampIndex(long timestamp);

    int lastId();

    int nextId(long timestamp);

    Iterable<Entry> getEntries();

    long sizeInMemory();

    long sizeOnDisk() throws Exception;

    void close();

    class Entry {

        public final long time;

        public final int index;

        public Entry(long time, int index) {
            this.time = time;
            this.index = index;
        }
    }
}
