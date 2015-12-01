package com.jivesoftware.os.miru.service.partition;

/**
 *
 */
public interface TrackError {

    void error(String reason);

    void reset();
}
