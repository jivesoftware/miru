package com.jivesoftware.os.miru.reader;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.logger.EndPointMetrics;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public class MiruHttpClientReader implements MiruReader {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final EndPointMetrics processMetrics;
    private final RequestHelper requestHelper;

    public MiruHttpClientReader(RequestHelper requestHelper) {
        this.requestHelper = requestHelper;
        this.processMetrics = new EndPointMetrics("process", LOG);
    }

    @Override
    public <P, R> R read(MiruTenantId tenantId, Optional<MiruActorId> userIdentity, P params, String endpoint, Class<R> resultClass, R defaultResult)
            throws MiruQueryServiceException {

        processMetrics.start();
        try {
            return requestHelper.executeRequest(params, endpoint, resultClass, defaultResult);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed filter custom stream", e);
        } finally {
            processMetrics.stop();
        }
    }

    @Override
    public void warm(MiruTenantId tenantId) throws MiruQueryServiceException {
        processMetrics.start();
        try {
            requestHelper.executeRequest(tenantId,
                    QUERY_SERVICE_ENDPOINT_PREFIX + WARM_ENDPOINT,
                    String.class, "");
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed count inbox unread stream", e);
        } finally {
            processMetrics.stop();
        }
    }

}
