package com.jivesoftware.os.miru.kinesis.logappender;

import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;

public class HttpKinesisLogAppender implements KinesisLogAppender, Appender {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final BlockingQueue<LogEvent> queue;
    private final int batchSize;
    private final boolean blocking;
    private final long ifSuccessPauseMillis;
    private final long ifEmptyPauseMillis;
    private final long ifErrorPauseMillis;
    private final int nonBlockingDrainThreshold;
    private final int nonBlockingDrainCount;

    private final String awsRegion;
    private final String awsAccessKeyId;
    private final String awsSecretAccessKey;
    private final String awsStreamName;

    private final AtomicBoolean installed = new AtomicBoolean(false);
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicReference<QueueConsumer> queueConsumer = new AtomicReference<>();
    private final Layout<?> layout = new EmptyLayout();

    private ErrorHandler errorHandler = new DefaultErrorHandler(this);
    private final ObjectMapper objectMapper = new ObjectMapper();

    HttpKinesisLogAppender(int queueSize,
        int batchSize,
        boolean blocking,
        long ifSuccessPauseMillis,
        long ifEmptyPauseMillis,
        long ifErrorPauseMillis,
        int nonBlockingDrainThreshold,
        int nonBlockingDrainCount,
        String awsRegion,
        String awsAccessKeyId,
        String awsSecretAccessKey,
        String awsStreamName) {
        this.queue = new ArrayBlockingQueue<>(queueSize);
        this.batchSize = batchSize;
        this.blocking = blocking;
        this.ifSuccessPauseMillis = ifSuccessPauseMillis;
        this.ifEmptyPauseMillis = ifEmptyPauseMillis;
        this.ifErrorPauseMillis = ifErrorPauseMillis;
        this.nonBlockingDrainThreshold = nonBlockingDrainThreshold;
        this.nonBlockingDrainCount = nonBlockingDrainCount;
        this.awsRegion = awsRegion;
        this.awsAccessKeyId = awsAccessKeyId;
        this.awsSecretAccessKey = awsSecretAccessKey;
        this.awsStreamName = awsStreamName;
    }

    public void install() {
        if (installed.compareAndSet(false, true)) {
            LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
            Configuration config = ctx.getConfiguration();
            config.addAppender(this);

            for (LoggerConfig loggerConfig : config.getLoggers().values()) {
                loggerConfig.addAppender(this, Level.INFO, null);
            }
        }

        start();
    }

    @Override
    public void append(LogEvent logEvent) {
        if (!isStarted()) {
            throw new IllegalStateException(getName() + " is not active");
        } else {
            if (blocking) {
                try {
                    queue.put(logEvent);
                } catch (InterruptedException ie) {
                    System.err.println("Interrupted while waiting for a free slot in the KinesisLogAppender LogEvent-queue " + getName());
                }
            } else {
                if (queue.remainingCapacity() < nonBlockingDrainThreshold) {
                    System.err.println("Draining to create space in the LogEvent-queue " + getName());
                    queue.drainTo(DEV_NULL_COLLECTION, nonBlockingDrainCount);
                }

                if (!queue.offer(logEvent)) {
                    System.err.println("Unable to write to full queue " + getName());
                }
            }
        }
    }

    @Override
    public String getName() {
        return "HttpKinesisLogAppender";
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
            BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey);
            AmazonKinesis client = AmazonKinesisClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials))
                .withRegion(awsRegion)
                .build();
            LOG.info("Connect to aws kinesis: region:{} key:{} stream:{}", awsRegion, awsAccessKeyId, awsStreamName);

            DescribeStreamResult describeStreamResult = client.describeStream(awsStreamName);
            if (describeStreamResult == null) {
                LOG.error("AWS Kinesis stream not found: " + awsStreamName);
                return;
            }
            LOG.info("AWS Kinesis stream " + awsStreamName + ": " + describeStreamResult.toString());

            while (running.get()) {
                List<LogEvent> logEventList = new ArrayList<>();
                queue.drainTo(logEventList, batchSize);
                LOG.trace("Process {} events of batch size {}", logEventList.size(), batchSize);

                if (logEventList.isEmpty()) {
                    try {
                        Thread.sleep(ifEmptyPauseMillis);
                    } catch (InterruptedException e) {
                        System.err.println("QueueConsumer was interrupted while sleeping due to empty queue");
                        Thread.interrupted();
                    }

                    LOG.inc(getName() + ">putRecords>empty", 1);
                } else {
                    while (true) {
                        try {
                            Collection<PutRecordsRequestEntry> records = new ArrayList<>();
                            for (LogEvent logEvent : logEventList) {
                                String toJson = objectMapper.writeValueAsString(logEvent);
                                LOG.trace("Put log event: " + toJson);
                                PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry()
                                    .withData(ByteBuffer.wrap(toJson.getBytes()))
                                    .withPartitionKey(String.format("%s-%d", getName(), System.currentTimeMillis()));
                                records.add(putRecordsRequestEntry);
                            }

                            PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
                            putRecordsRequest.setRecords(records);
                            putRecordsRequest.setStreamName(awsStreamName);
                            PutRecordsResult putRecordsResult = client.putRecords(putRecordsRequest);
                            LOG.trace(putRecordsResult.toString());

                            LOG.inc(getName() + ">putRecords>success", 1);
                            LOG.inc(getName() + ">putRecords>size", records.size());

                            break;
                        } catch (Exception e) {
                            System.err.println("Append failed for logger: " + e.getClass().getCanonicalName() + ": " + e.getMessage());
                        }

                        try {
                            Thread.sleep(ifErrorPauseMillis);
                        } catch (InterruptedException e) {
                            System.err.println("QueueConsumer was interrupted while sleeping due to errors");
                            Thread.interrupted();
                        }

                        LOG.inc(getName() + ">putRecords>failure", 1);
                    }

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

    private static final DevNullCollection<LogEvent> DEV_NULL_COLLECTION = new DevNullCollection<>();

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
