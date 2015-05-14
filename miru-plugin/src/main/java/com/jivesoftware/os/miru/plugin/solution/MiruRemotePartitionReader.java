package com.jivesoftware.os.miru.plugin.solution;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.HttpClient;
import com.jivesoftware.os.jive.utils.http.client.HttpClientException;
import com.jivesoftware.os.jive.utils.http.client.HttpResponse;
import com.jivesoftware.os.jive.utils.http.client.rest.NonSuccessStatusCodeException;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.mlogger.core.EndPointMetrics;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.commons.httpclient.HttpStatus;

/**
 *
 */
public class MiruRemotePartitionReader<Q, A, R> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final ObjectMapper mapper = new ObjectMapper(); // HACK

    private final MiruRemotePartition<Q, A, R> remotePartition;
    private final HttpClient client;
    private final MiruSolutionMarshaller<Q, A, R> marshaller;

    public MiruRemotePartitionReader(MiruRemotePartition<Q, A, R> remotePartition, HttpClient client, MiruSolutionMarshaller<Q, A, R> marshaller) {
        this.remotePartition = remotePartition;
        this.client = client;
        this.marshaller = marshaller;
    }

    public MiruPartitionResponse<A> read(MiruPartitionId partitionId, MiruRequest<Q> request, Optional<R> report)
        throws MiruQueryServiceException, MiruPartitionUnavailableException {

        EndPointMetrics endpointMetrics = remotePartition.getEndpointMetrics();
        endpointMetrics.start();
        try {
            MiruRequestAndReport<Q, R> params = new MiruRequestAndReport<>(request, report.orNull());
//            return requestHelper.executeRequest(params, remotePartition.getEndpoint(partitionId),
//                MiruPartitionResponse.class, new Class[] { remotePartition.getAnswerClass() },
//                new MiruPartitionResponse<>(remotePartition.getEmptyResults(), null));

            String requestJsonString = mapper.writeValueAsString(params);
            HttpResponse response = client.postJson(remotePartition.getEndpoint(partitionId), requestJsonString);
            int statusCode = response.getStatusCode();
            if (statusCode < 200 || statusCode >= 300) {
                return null;
            }
            JavaType responseType = mapper.getTypeFactory().constructParametricType(MiruPartitionResponse.class, remotePartition.getAnswerClass());
            return mapper.readValue(new ByteArrayInputStream(response.getResponseBody()), responseType);
            /*
             byte[] requestAsBytes = marshaller.requestAndReportToBytes(params);
             HttpResponse response = client.postBytes(remotePartition.getEndpoint(partitionId), requestAsBytes);
             int statusCode = response.getStatusCode();
             if (statusCode < 200 || statusCode >= 300) {
             return null;
             }
             return marshaller.responseFromStream(new ByteArrayInputStream(response.getResponseBody()));*/

        } catch (NonSuccessStatusCodeException e) {
            if (e.getStatusCode() == HttpStatus.SC_SERVICE_UNAVAILABLE) {
                throw new MiruPartitionUnavailableException("Remote partition is unavailable");
            } else {
                throw new MiruQueryServiceException("Failed remote read for partition: " + partitionId.getId(), e);
            }
        } catch (HttpClientException | RuntimeException e) {
            throw new MiruQueryServiceException("Failed remote read for partition: " + partitionId.getId(), e);
        } catch (IOException ex) {
            LOG.error("Failed to marshall request or response.", ex);
            return null;
        } finally {
            endpointMetrics.stop();
        }
    }
}
