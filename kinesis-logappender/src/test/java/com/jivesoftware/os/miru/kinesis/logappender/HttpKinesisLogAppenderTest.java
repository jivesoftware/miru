package com.jivesoftware.os.miru.kinesis.logappender;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;

import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(enabled = false)
public class HttpKinesisLogAppenderTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String awsStreamName = "<stream name>";

    private AmazonKinesis client;

    @Test
    public void testLogEventJson() throws Exception {
        LogEvent le = Log4jLogEvent.createEvent("foobar", null, "foo.bar", Level.ERROR, null, null, null, null, null, "tname", null, 12345);

        List<LogEvent> logEventList = new ArrayList<>();
        logEventList.add(le);

        String toJson = objectMapper.writeValueAsString(logEventList);
        System.out.println(toJson);
    }

    @BeforeClass
    public void beforeClass() {
        String awsRegion = "<region>";
        String awsAccessKeyId = "<access key id>";
        String awsSecretAccessKey = "<secret access key>";

        BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey);
        client = AmazonKinesisClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials))
            .withRegion(awsRegion)
            .build();
    }

    @Test
    public void testKinesisConnection() {
        DescribeStreamResult describeStreamResult = client.describeStream(awsStreamName);
        System.out.println("AWS Kinesis stream " + awsStreamName + ": " +
            (describeStreamResult == null ? "null" : describeStreamResult.toString()));
    }

    @Test
    public void testKinesisPutRecords() throws Exception {
        List<LogEvent> logEventList = Collections.singletonList(
            Log4jLogEvent.createEvent("foobar", null, "foo.bar", Level.ERROR, null, null, null, null, null, "tname", null, 12345));

        Collection<PutRecordsRequestEntry> records = new ArrayList<>();
        for (LogEvent logEvent : logEventList) {
            String toJson = objectMapper.writeValueAsString(logEvent);
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry()
                .withData(ByteBuffer.wrap(toJson.getBytes()))
                .withPartitionKey("testKinesisPutRecords");
            records.add(putRecordsRequestEntry);
        }

        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setRecords(records);
        putRecordsRequest.setStreamName(awsStreamName);
        PutRecordsResult putRecordsResult = client.putRecords(putRecordsRequest);
        System.out.println(putRecordsResult.toString());
    }

}
