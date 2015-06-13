package com.jivesoftware.os.miru.plugin.solution;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.mlogger.core.EndPointMetrics;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelper;
import com.jivesoftware.os.routing.bird.http.client.NonSuccessStatusCodeException;
import org.apache.http.HttpStatus;

/**
 *
 */
public class JsonRemotePartitionReader implements MiruRemotePartitionReader {

    private static final ObjectMapper MAPPER;

    static {
        MAPPER = new ObjectMapper();
        MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        MAPPER.registerModule(new GuavaModule());
    }

    @Override
    public <Q, A, P> MiruPartitionResponse<A> read(HttpClient httpClient,
        String endpoint,
        MiruRequest<Q> request,
        Class<A> answerClass,
        Optional<P> report,
        EndPointMetrics endPointMetrics,
        A emptyResults) throws MiruQueryServiceException, MiruPartitionUnavailableException {

        endPointMetrics.start();
        try {
            MiruRequestAndReport<Q, P> params = new MiruRequestAndReport<>(request, report.orNull());
            HttpRequestHelper requestHelper = new HttpRequestHelper(httpClient, MAPPER);
            return requestHelper.executeRequest(params, endpoint,
                MiruPartitionResponse.class, new Class[] { answerClass },
                new MiruPartitionResponse<>(emptyResults, null));
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
}
