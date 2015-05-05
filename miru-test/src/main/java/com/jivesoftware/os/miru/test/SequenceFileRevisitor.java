package com.jivesoftware.os.miru.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import java.io.EOFException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 *
 */
public class SequenceFileRevisitor implements MiruTestActivityDistributor.Revisitor {

    private final Configuration hadoopConfiguration;
    private final String outputPath;
    private final String tenantId;
    private SequenceFile.Reader reader;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public SequenceFileRevisitor(Configuration hadoopConfiguration, String outputPath, String tenantId) {
        this.hadoopConfiguration = hadoopConfiguration;
        this.outputPath = outputPath;
        this.tenantId = tenantId;
    }

    @Override
    public void skip() throws IOException {
        getReader().next(new LongWritable(), new Text());
    }

    @Override
    public MiruPartitionedActivity visit() throws IOException {
        Text json = new Text();
        if (getReader().next(new LongWritable(), json)) {
            return objectMapper.readValue(json.getBytes(), MiruPartitionedActivity.class);
        } else {
            throw new EOFException();
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    synchronized private SequenceFile.Reader getReader() throws IOException {
        if (reader == null) {
            Path path = new Path(outputPath, tenantId + ".activity");
            reader = new SequenceFile.Reader(hadoopConfiguration, SequenceFile.Reader.file(path));
        }
        return reader;
    }
}
