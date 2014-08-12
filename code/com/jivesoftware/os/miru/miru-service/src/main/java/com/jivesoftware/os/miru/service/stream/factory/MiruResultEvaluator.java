package com.jivesoftware.os.miru.service.stream.factory;

/**
 *
 */
public interface MiruResultEvaluator<R> {

    boolean isDone(R result);
}
