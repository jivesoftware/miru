package com.jivesoftware.os.miru.stumptown.deployable;

import com.jivesoftware.os.miru.logappender.MiruLogEvent;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.ErrorHandler;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.DefaultErrorHandler;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.ByteBufferDestination;

/**
 *
 */
public class MiruStumptownInternalLogAppender implements Appender {

    private final String datacenter;
    private final String cluster;
    private final String host;
    private final String service;
    private final String instance;
    private final String version;
    private final MiruStumptownIntakeService intakeService;

    private final BlockingQueue<MiruLogEvent> queue;
    private final int batchSize;
    private final boolean blocking;
    private final long ifSuccessPauseMillis;
    private final long ifEmptyPauseMillis;
    private final long ifErrorPauseMillis;
    private final int nonBlockingDrainThreshold;
    private final int nonBlockingDrainCount;

    private final AtomicBoolean installed = new AtomicBoolean(false);
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicReference<QueueConsumer> queueConsumer = new AtomicReference<>();
    private final Layout<?> layout = new EmptyLayout();

    private ErrorHandler errorHandler = new DefaultErrorHandler(this);

    public MiruStumptownInternalLogAppender(String datacenter,
        String cluster,
        String host,
        String service,
        String instance,
        String version,
        MiruStumptownIntakeService intakeService,
        int queueSize,
        int batchSize,
        boolean blocking,
        long ifSuccessPauseMillis,
        long ifEmptyPauseMillis,
        long ifErrorPauseMillis,
        int nonBlockingDrainThreshold,
        int nonBlockingDrainCount) {
        this.datacenter = datacenter;
        this.host = host;
        this.service = service;
        this.instance = instance;
        this.version = version;
        this.intakeService = intakeService;
        this.cluster = cluster;
        this.queue = new ArrayBlockingQueue<>(queueSize);
        this.batchSize = batchSize;
        this.blocking = blocking;
        this.ifSuccessPauseMillis = ifSuccessPauseMillis;
        this.ifEmptyPauseMillis = ifEmptyPauseMillis;
        this.ifErrorPauseMillis = ifErrorPauseMillis;
        this.nonBlockingDrainThreshold = nonBlockingDrainThreshold;
        this.nonBlockingDrainCount = nonBlockingDrainCount;
    }

    public void install() {
        if (installed.compareAndSet(false, true)) {
            LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
            Configuration config = ctx.getConfiguration();
            config.addAppender(this);

            for (LoggerConfig loggerConfig : config.getLoggers().values()) {
                loggerConfig.addAppender(this, Level.INFO, null); // TODO expose to config
            }
        }

        start();
    }

    @Override
    public void append(LogEvent logEvent) {
        if (!isStarted()) {
            throw new IllegalStateException("MiruLogAppender " + getName() + " is not active");
        } else {
            for (StackTraceElement traceElement : Thread.currentThread().getStackTrace()) {
                // Kills recusive logging problem.
                if (traceElement.getClassName().endsWith("MiruStumptownIntakeService")) {
                    return;
                }
            }

            String methodName = null;
            String lineNumber = null;
            StackTraceElement source = logEvent.getSource();
            if (source != null) {
                methodName = source.getMethodName();
                lineNumber = String.valueOf(source.getLineNumber());
            }

            String exceptionClass = null;
            Throwable thrown = logEvent.getThrown();
            if (thrown != null) {
                exceptionClass = thrown.getClass().getCanonicalName();
            }

            MiruLogEvent miruLogEvent = new MiruLogEvent(datacenter,
                cluster,
                host,
                service,
                instance,
                version,
                logEvent.getLevel().name(),
                logEvent.getThreadName(),
                logEvent.getLoggerName(),
                methodName,
                lineNumber,
                logEvent.getMessage().getFormattedMessage(),
                String.valueOf(logEvent.getTimeMillis()),
                exceptionClass,
                toStackTrace(logEvent.getThrown()));

            if (blocking) {
                try {
                    queue.put(miruLogEvent);
                } catch (InterruptedException ie) {
                    System.err.println("Interrupted while waiting for a free slot in the MiruLogAppender MiruLogEvent-queue " + getName());
                }
            } else {
                if (queue.remainingCapacity() < nonBlockingDrainThreshold) {
                    System.err.println("Draining to create space in the MiruLogAppender MiruLogEvent-queue " + getName());
                    queue.drainTo(DEV_NULL_COLLECTION, nonBlockingDrainCount);
                }
                boolean appendSuccessful = queue.offer(miruLogEvent);
                if (appendSuccessful) {
                } else {
                    System.err.println("MiruLogAppender " + getName() + " is unable to write. Queue is full!");
                }
            }
        }
    }

    private String[] toStackTrace(Throwable throwable) {
        if (throwable == null) {
            return null;
        }

        StackTraceElement[] elements = throwable.getStackTrace();
        String[] stackTrace = new String[elements.length];
        for (int i = 0; i < elements.length; i++) {
            stackTrace[i] = elements[i].toString();
        }
        return stackTrace;
    }

    @Override
    public String getName() {
        return cluster + "," + host + "," + instance + "," + service + "," + version;
    }

    @Override
    public Layout<? extends Serializable> getLayout() {
        return layout;
    }

    @Override
    public boolean ignoreExceptions() {
        return true;
    }

    @Override
    public ErrorHandler getHandler() {
        return errorHandler;
    }

    @Override
    public void setHandler(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    @Override
    public State getState() {
        return State.INITIALIZED;
    }

    @Override
    public void initialize() {

    }

    @Override
    public void start() {
        if (started.compareAndSet(false, true)) {
            QueueConsumer consumer = new QueueConsumer();
            Thread thread = new Thread(consumer);
            thread.start();
            queueConsumer.set(consumer);
        }
    }

    @Override
    public void stop() {
        if (started.compareAndSet(true, false)) {
            QueueConsumer consumer = queueConsumer.getAndSet(null);
            if (consumer != null) {
                consumer.stop();
            }
        }
    }

    @Override
    public boolean isStarted() {
        return started.get();
    }

    @Override
    public boolean isStopped() {
        return !started.get();
    }

    private class QueueConsumer implements Runnable {

        private final AtomicBoolean running = new AtomicBoolean(true);

        @Override
        public void run() {
            List<MiruLogEvent> events = new ArrayList<>();
            while (running.get()) {
                queue.drainTo(events, batchSize);
                if (events.isEmpty()) {
                    try {
                        Thread.sleep(ifEmptyPauseMillis);
                    } catch (InterruptedException e) {
                        System.err.println("QueueConsumer was interrupted while sleeping due to empty queue");
                        Thread.interrupted();
                    }
                } else {
                    deliver:
                    while (true) {
                        try {
                            intakeService.ingressLogEvents(events);
                            break deliver;
                        } catch (Exception e) {
                            System.err.println("Append failed for logger: " + e.getMessage());
                        }

                        try {
                            Thread.sleep(ifErrorPauseMillis);
                        } catch (InterruptedException e) {
                            System.err.println("QueueConsumer was interrupted while sleeping due to errors");
                            Thread.interrupted();
                        }
                    }

                    events.clear();

                    try {
                        Thread.sleep(ifSuccessPauseMillis);
                    } catch (InterruptedException e) {
                        System.err.println("QueueConsumer was interrupted while sleeping after success");
                        Thread.interrupted();
                    }
                }
            }
        }

        private void stop() {
            running.set(false);
        }
    }

    private static final DevNullCollection<MiruLogEvent> DEV_NULL_COLLECTION = new DevNullCollection<>();

    private static class DevNullCollection<E> implements Collection<E> {

        private DevNullCollection() {
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public boolean contains(Object o) {
            return false;
        }

        @Override
        public Iterator<E> iterator() {
            return Collections.EMPTY_LIST.iterator();
        }

        @Override
        public Object[] toArray() {
            return new Object[0];
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return a;
        }

        @Override
        public boolean add(E e) {
            return true;
        }

        @Override
        public boolean remove(Object o) {
            return false;
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return false;
        }

        @Override
        public boolean addAll(Collection<? extends E> c) {
            return true;
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return false;
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return false;
        }

        @Override
        public void clear() {
        }
    }

    private static class EmptyLayout implements Layout<Serializable> {

        @Override
        public byte[] getFooter() {
            return new byte[0];
        }

        @Override
        public byte[] getHeader() {
            return new byte[0];
        }

        @Override
        public byte[] toByteArray(LogEvent logEvent) {
            return new byte[0];
        }

        @Override
        public Serializable toSerializable(LogEvent logEvent) {
            return logEvent;
        }

        @Override
        public String getContentType() {
            return "empty";
        }

        @Override
        public Map<String, String> getContentFormat() {
            return Collections.emptyMap();
        }

        @Override
        public void encode(LogEvent logEvent, ByteBufferDestination byteBufferDestination) {

        }
    }
}
