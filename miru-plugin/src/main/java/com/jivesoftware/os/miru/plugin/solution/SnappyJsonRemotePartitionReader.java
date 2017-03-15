package com.jivesoftware.os.miru.plugin.solution;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruHostSelectiveStrategy;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.mlogger.core.EndPointMetrics;
import com.jivesoftware.os.routing.bird.shared.HttpClientException;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.NonSuccessStatusCodeException;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import java.util.Map;
import org.apache.http.HttpStatus;
import org.xerial.snappy.Snappy;

public class SnappyJsonRemotePartitionReader implements MiruRemotePartitionReader {

    private static final byte[] EMPTY_RESPONSE = new byte[0];
    private static final ObjectMapper MAPPER;
    private static final HttpResponseMapper RESPONSE_MAPPER;

    static {
        MAPPER = new ObjectMapper();
        MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        MAPPER.registerModule(new GuavaModule());

        RESPONSE_MAPPER = new HttpResponseMapper(MAPPER);
    }

    private final TenantAwareHttpClient<String> readerHttpClient;
    private final Map<MiruHost, MiruHostSelectiveStrategy> strategyPerHost;

    public SnappyJsonRemotePartitionReader(TenantAwareHttpClient<String> readerHttpClient,
        Map<MiruHost, MiruHostSelectiveStrategy> strategyPerHost) {
        this.readerHttpClient = readerHttpClient;
        this.strategyPerHost = strategyPerHost;
    }

    public <Q, A, P> MiruPartitionResponse<A> read(String queryKey,
        MiruHost host,
        String endpoint,
        MiruRequest<Q> request,
        Class<A> answerClass,
        Optional<P> report,
        EndPointMetrics endPointMetrics,
        A emptyResults)
        throws MiruQueryServiceException, MiruPartitionUnavailableException {

        endPointMetrics.start();
        try {
            MiruHostSelectiveStrategy strategy = strategyPerHost.computeIfAbsent(host,
                miruHost -> new MiruHostSelectiveStrategy(new MiruHost[] { miruHost }));
            MiruRequestAndReport<Q, P> requestAndReport = new MiruRequestAndReport<>(request, report.orNull());
            byte[] postBytes = pack(requestAndReport);
            return unpack(
                readerHttpClient.call("", strategy,
                    queryKey + ":" + request.name + ":snappyJson",
                    httpClient1 -> {
                        HttpResponse httpResponse = httpClient1.postBytes(endpoint, postBytes, null);
                        if (!RESPONSE_MAPPER.isSuccessStatusCode(httpResponse.getStatusCode())) {
                            throw new NonSuccessStatusCodeException(httpResponse.getStatusCode(), "Non success status code: " + httpResponse.getStatusCode());
                        }

                        byte[] responseBody = httpResponse.getResponseBody();
                        if (responseBody == null) {
                            responseBody = EMPTY_RESPONSE;
                        }

                        return new ClientCall.ClientResponse<>(responseBody, true);
                    }),
                answerClass,
                emptyResults);
        } catch (NonSuccessStatusCodeException e) {
            if (e.getStatusCode() == HttpStatus.SC_SERVICE_UNAVAILABLE) {
                throw new MiruPartitionUnavailableException("Remote partition is unavailable for host: " + host + " endpoint: " + e.getMessage());
            } else {
                throw new MiruQueryServiceException("Failed remote read for host: " + host + " endpoint: " + endpoint, e);
            }
        } catch (HttpClientException e) {
            throw new MiruQueryServiceException("Failed to query host: " + host + " endpoint: " + endpoint, e);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed remote read for host: " + host + " endpoint: " + endpoint, e);
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
}
