package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class MiruRebuildDirector {

    private final AtomicLong activityCount;

    public MiruRebuildDirector(long maxConcurrentActivityCount) {
        this.activityCount = new AtomicLong(maxConcurrentActivityCount);
    }

    public Optional<Token> acquire(long count) {
        if (activityCount.get() >= count) {
            synchronized (activityCount) {
                if (activityCount.get() >= count) {
                    activityCount.addAndGet(-count);
                    return Optional.of(new Token(count));
                }
            }
        }
        return Optional.absent();
    }

    public void release(Token token) {
        activityCount.addAndGet(token.count);
    }

    public long available() {
        return activityCount.get();
    }

    public static class Token {
        private final long count;

        private Token(long count) {
            this.count = count;
        }
    }
}
