package com.jivesoftware.os.miru.service.index;

/**
 *
 */
public interface TimeOrderAnomalyStream {

    void underflowOfSmallestTimestamp(long delta);

    void underflowOfLargestTimestamp(long delta);

}
