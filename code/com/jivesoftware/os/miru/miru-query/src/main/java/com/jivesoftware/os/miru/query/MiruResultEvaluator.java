package com.jivesoftware.os.miru.query;

/**
 *
 */
public interface MiruResultEvaluator<R> {

    boolean isDone(R result);
}
