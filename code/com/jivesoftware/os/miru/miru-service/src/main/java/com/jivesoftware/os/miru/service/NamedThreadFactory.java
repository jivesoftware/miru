package com.jivesoftware.os.miru.service;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
*
*/
public class NamedThreadFactory implements ThreadFactory {

    private final ThreadGroup g;
    private final String namePrefix;
    private final AtomicLong threadNumber;

    public NamedThreadFactory(ThreadGroup g, String name) {
        this.g = g;
        namePrefix = name + "-";
        threadNumber = new AtomicLong();
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(g, r,
            namePrefix + threadNumber.getAndIncrement(),
            0);
        if (t.isDaemon()) {
            t.setDaemon(false);
        }
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }
        return t;
    }
}
