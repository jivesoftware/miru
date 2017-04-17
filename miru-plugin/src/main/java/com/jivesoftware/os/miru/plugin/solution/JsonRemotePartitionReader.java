package com.jivesoftware.os.miru.plugin.solution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruHostSelectiveStrategy;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.mlogger.core.EndPointMetrics;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.NonSuccessStatusCodeException;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.routing.bird.shared.HttpClientException;
import java.util.Map;
import org.apache.http.HttpStatus;

public class JsonRemotePartitionReader implements MiruRemotePartitionReader {

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

    public JsonRemotePartitionReader(TenantAwareHttpClient<String> readerHttpClient, Map<MiruHost, MiruHostSelectiveStrategy> strategyPerHost) {
        this.readerHttpClient = readerHttpClient;
        this.strategyPerHost = strategyPerHost;
    }

    @Override
    public <Q, A, P> MiruPartitionResponse<A> read(String queryKey,
        MiruHost host,
        String endpoint,
        MiruRequest<Q> request,
        Class<A> answerClass,
        Optional<P> report,
        EndPointMetrics endPointMetrics,
        A emptyResults) throws MiruQueryServiceException, MiruPartitionUnavailableException {

        endPointMetrics.start();
        try {
            MiruHostSelectiveStrategy strategy = strategyPerHost.computeIfAbsent(host,
                miruHost -> new MiruHostSelectiveStrategy(new MiruHost[] { miruHost }));
            MiruRequestAndReport<Q, P> params = new MiruRequestAndReport<>(request, report.orNull());
            String jsonParams = MAPPER.writeValueAsString(params);
            return readerHttpClient.call("", strategy,
                queryKey + ":" + request.name + ":json",
                httpClient1 -> {
                    HttpResponse httpResponse = httpClient1.postJson(endpoint, jsonParams, null);
                    if (!RESPONSE_MAPPER.isSuccessStatusCode(httpResponse.getStatusCode())) {
                        throw new NonSuccessStatusCodeException(httpResponse.getStatusCode(), "Non success status code: " + httpResponse.getStatusCode());
                    }
                    @SuppressWarnings("unchecked")
                    MiruPartitionResponse<A> response = RESPONSE_MAPPER.extractResultFromResponse(httpResponse,
                        MiruPartitionResponse.class, new Class[] { answerClass },
                        new MiruPartitionResponse<>(emptyResults, null));
                    return new ClientCall.ClientResponse<>(response, true);
                });
        } catch (NonSuccessStatusCodeException e) {
            if (e.getStatusCode() == HttpStatus.SC_SERVICE_UNAVAILABLE) {
                throw new MiruPartitionUnavailableException("Remote partition is unavailable for host: " + host + " endpoint: " + e.getMessage());
            } else {
                throw new MiruQueryServiceException("Failed remote read for host: " + host + " endpoint: " + endpoint, e);
            }
        } catch (HttpClientException e) {
            throw new MiruQueryServiceException("Failed to query host: " + host + " endpoint: " + endpoint, e);
        } catch (JsonProcessingException e) {
            throw new MiruQueryServiceException("Failed to serialize params for host: " + host + " endpoint: " + endpoint, e);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed remote read for host: " + host + " endpoint: " + endpoint, e);
        } finally {
            endPointMetrics.stop();
        }
    }
}
