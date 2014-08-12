package com.jivesoftware.os.miru.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
*
*/
public class QueryConsumerRunnable implements Runnable {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final SequenceFile.Writer writer;
    private final BlockingQueue<Object> queue;
    private final AtomicBoolean done;

    public QueryConsumerRunnable(SequenceFile.Writer writer, BlockingQueue<Object> queue, AtomicBoolean done) {
        this.writer = writer;
        this.queue = queue;
        this.done = done;
    }

    @Override
    public void run() {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            AtomicInteger count = new AtomicInteger();
            while (!done.get() || !queue.isEmpty()) {
                List<Object> queries = Lists.newArrayList();
                queue.drainTo(queries);

                for (Object query : queries) {
                    String json = objectMapper.writeValueAsString(query);
                    log.debug(json);
                    writer.append(new LongWritable(count.getAndIncrement()), new Text(json));
                }
            }
        } catch (Exception e) {
            log.error("Query consumer died", e);
        }
    }
}
