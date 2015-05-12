package com.jivesoftware.os.miru.plugin.solution;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 * @author jonathan.colt
 */
public class JacksonMiruSolutionMarshaller<Q, A, R> implements MiruSolutionMarshaller<Q, A, R> {

    private final ObjectMapper mapper;
    private final JavaType requestType;
    private final JavaType responseType;

    public JacksonMiruSolutionMarshaller(ObjectMapper mapper, Class<Q> questionClass, Class<A> answerClass, Class<R> reportClass) {
        this.mapper = mapper;
        this.requestType = mapper.getTypeFactory().constructParametricType(MiruRequestAndReport.class, questionClass, reportClass);
        this.responseType = mapper.getTypeFactory().constructParametricType(MiruPartitionResponse.class, answerClass);
    }

    @Override
    public MiruRequestAndReport<Q, R> requestAndReportFromBytes(byte[] requestBytes) throws IOException {
        return mapper.readValue(requestBytes, requestType);
    }

    @Override
    public byte[] requestAndReportToBytes(MiruRequestAndReport<Q, R> requestAndReport) throws IOException {
        return mapper.writeValueAsBytes(requestAndReport);
    }

    @Override
    public MiruPartitionResponse<A> responseFromStream(InputStream is) throws IOException {
        return mapper.readValue(is, responseType);
    }

    @Override
    public void responseToStream(MiruPartitionResponse<A> response, OutputStream os) throws IOException {
        mapper.writeValue(os, response);
    }
}
