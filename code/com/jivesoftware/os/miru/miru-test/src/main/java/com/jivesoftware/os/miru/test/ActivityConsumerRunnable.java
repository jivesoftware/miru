package com.jivesoftware.os.miru.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 *
 */
public class ActivityConsumerRunnable implements Runnable {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final SequenceFile.Writer writer;
    private final BlockingQueue<MiruPartitionedActivity> queue;
    private final AtomicBoolean done;

    public ActivityConsumerRunnable(SequenceFile.Writer writer, BlockingQueue<MiruPartitionedActivity> queue, AtomicBoolean done) {
        this.writer = writer;
        this.queue = queue;
        this.done = done;
    }

    @Override
    public void run() {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            while (!done.get() || !queue.isEmpty()) {
                List<MiruPartitionedActivity> activities = Lists.newArrayList();
                queue.drainTo(activities);

                for (MiruPartitionedActivity activity : activities) {
                    String json = objectMapper.writeValueAsString(activity);
                    log.debug(json);
                    writer.append(new LongWritable(activity.timestamp), new Text(json));
                }
            }
        } catch (Exception e) {
            log.error("Activity consumer died", e);
        }
    }
}
