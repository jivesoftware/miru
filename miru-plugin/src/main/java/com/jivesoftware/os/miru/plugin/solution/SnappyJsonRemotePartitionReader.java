package com.jivesoftware.os.miru.plugin.solution;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.HttpClient;
import com.jivesoftware.os.jive.utils.http.client.HttpClientException;
import com.jivesoftware.os.jive.utils.http.client.HttpResponse;
import com.jivesoftware.os.jive.utils.http.client.rest.NonSuccessStatusCodeException;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.mlogger.core.EndPointMetrics;
import org.apache.commons.httpclient.HttpStatus;
import org.xerial.snappy.Snappy;

/**
 *
 */
public class SnappyJsonRemotePartitionReader implements MiruRemotePartitionReader {

    private static final byte[] EMPTY_RESPONSE = new byte[0];
    private static final ObjectMapper MAPPER;
    static {
        MAPPER = new ObjectMapper();
        MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        MAPPER.registerModule(new GuavaModule());
    }

    public <Q, A, P> MiruPartitionResponse<A> read(HttpClient httpClient,
        String endpoint,
        MiruRequest<Q> request,
        Class<A> answerClass,
        Optional<P> report,
        EndPointMetrics endPointMetrics,
        A emptyResults)
        throws MiruQueryServiceException, MiruPartitionUnavailableException {

        endPointMetrics.start();
        try {
            MiruRequestAndReport<Q, P> requestAndReport = new MiruRequestAndReport<>(request, report.orNull());
            return unpack(executePost(httpClient, endpoint, pack(requestAndReport)), answerClass, emptyResults);
        } catch (NonSuccessStatusCodeException e) {
            if (e.getStatusCode() == HttpStatus.SC_SERVICE_UNAVAILABLE) {
                throw new MiruPartitionUnavailableException("Remote partition is unavailable");
            } else {
                throw new MiruQueryServiceException("Failed remote read for endpoint: " + endpoint, e);
            }
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed remote read for endpoint: " + endpoint, e);
        } finally {
            endPointMetrics.stop();
        }
    }

    private <Q, P> byte[] pack(MiruRequestAndReport<Q, P> requestAndReport) {
        try {
            return Snappy.compress(MAPPER.writeValueAsBytes(requestAndReport));
        } catch (Exception x) {
            throw new RuntimeException("Error serializing request parameters object for request of type: " +
                requestAndReport.request.query.getClass().getSimpleName(), x);
        }
    }

    private <A> MiruPartitionResponse<A> unpack(byte[] compressedBytes, Class<A> answerClass, A emptyResult) {
        if (compressedBytes == null || compressedBytes.length == 0) {
            return new MiruPartitionResponse<>(emptyResult, null);
        }
        try {
            JavaType resultType = MAPPER.getTypeFactory().constructParametricType(MiruPartitionResponse.class, answerClass);
            return MAPPER.readValue(Snappy.uncompress(compressedBytes), resultType);
        } catch (Exception x) {
            throw new RuntimeException("Error deserializing response object for bytes of length=" + compressedBytes.length, x);
        }
    }

    private byte[] executePost(HttpClient httpClient, String endpointUrl, byte[] postEntity) {
        HttpResponse response;
        try {
            response = httpClient.postBytes(endpointUrl, postEntity);
        } catch (HttpClientException x) {
            throw new RuntimeException("Error posting query request to server for entity length=" + postEntity.length +
                " at endpoint=\"" + endpointUrl + "\".", x);
        }

        byte[] responseBody = response.getResponseBody();
        if (responseBody == null) {
            responseBody = EMPTY_RESPONSE;
        }

        if (!this.isSuccessStatusCode(response.getStatusCode())) {
            throw new NonSuccessStatusCodeException(response.getStatusCode(),
                "Received non success status code (" + response.getStatusCode() + ") " + "from the server.  The reason phrase on the response was \"" +
                    response.getStatusReasonPhrase() + "\" " + "and the body of the response was \"" + new String(responseBody, Charsets.UTF_8) + "\".");
        } else {
            return responseBody;
        }
    }

    private boolean isSuccessStatusCode(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }
}
