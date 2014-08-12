package com.jivesoftware.os.miru.service.partition;

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
