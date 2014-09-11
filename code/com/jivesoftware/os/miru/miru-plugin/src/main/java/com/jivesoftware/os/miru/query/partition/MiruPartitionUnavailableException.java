package com.jivesoftware.os.miru.query.partition;

/**
 *
 */
public class MiruPartitionUnavailableException extends RuntimeException {

    public MiruPartitionUnavailableException(String message) {
        super(message);
    }

    public MiruPartitionUnavailableException(Throwable cause) {
        super(cause);
    }
}
