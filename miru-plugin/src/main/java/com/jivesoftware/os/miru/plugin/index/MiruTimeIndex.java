/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.io.IOException;
import java.util.List;

/**
 * @author jonathan
 */
public interface MiruTimeIndex {

    boolean[] contains(List<Long> timestamps, StackBuffer stackBuffer) throws Exception;

    boolean intersects(MiruTimeRange timeRange);

    /**
     * Returns the actual index of the given timestamp if it appears in the index, or else where it would have been.
     *
     * @param timestamp the activity timestamp
     * @return the actual index or nearest insertion point
     */
    int getClosestId(long timestamp, StackBuffer stackBuffer) throws IOException, InterruptedException;

    int getExactId(long timestamp, StackBuffer stackBuffer) throws Exception;

    long getLargestTimestamp() throws IOException, InterruptedException;

    long getSmallestTimestamp() throws IOException, InterruptedException;

    long getTimestamp(int id, StackBuffer stackBuffer) throws IOException, InterruptedException;

    /**
     * Returns the smallest id satisfying the condition that {@link #getTimestamp(int, StackBuffer)}
     * is greater than the requested timestamp (exclusive lower bound).
     *
     * @param timestamp the timestamp serving as the exclusive lower bound
     * @return the smallest id exclusive of the requested timestamp
     */
    int smallestExclusiveTimestampIndex(long timestamp, StackBuffer stackBuffer) throws IOException, InterruptedException;

    /**
     * Returns the largest id satisfying the condition that {@link #getTimestamp(int, StackBuffer)}
     * is less than or equal to the requested timestamp (inclusive upper bound).
     *
     * @param timestamp the timestamp serving as the inclusive upper bound
     * @return the largest id inclusive of the requested timestamp
     */
    int largestInclusiveTimestampIndex(long timestamp, StackBuffer stackBuffer) throws IOException, InterruptedException;

    int lastId();

    int[] nextId(StackBuffer stackBuffer, long... timestamps) throws Exception;

    void close();

}
