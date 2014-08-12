package com.jivesoftware.os.miru.deployable;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.jivesoftware.jive.deployer.server.http.jetty.jersey.endpoints.testable.SelfTestHealthCheck;
import com.jivesoftware.jive.platform.events.id.SystemUser;
import com.jivesoftware.jive.platform.events.id.UserIdentity;
import com.jivesoftware.os.miru.api.MiruDistinctCountQueryCriteria;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.api.query.result.DistinctCountResult;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;

@Singleton
public class MiruReaderHealthCheck {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger(MiruReaderHealthCheck.class);

    private final MiruTenantId tenantId = new MiruTenantId("brewspace-noggin".getBytes(Charsets.UTF_8));
    private final Id actorId = SystemUser.id();
    private final MiruReader miruReader;

    @Inject
    public MiruReaderHealthCheck(MiruReader miruReader) {
        this.miruReader = miruReader;
    }

    @Test(groups = SelfTestHealthCheck.GROUP)
    public void get() throws Exception {
        final String timerName = "get";
        LOG.startTimer(timerName);
        try {
            DistinctCountResult result = miruReader.countCustomStream(
                tenantId,
                Optional.of(new UserIdentity(actorId)),
                Optional.<MiruAuthzExpression>absent(),
                new MiruDistinctCountQueryCriteria.Builder()
                    .setStreamFilter(new MiruFilter(
                        MiruFilterOperation.or,
                        Optional.of(ImmutableList.of(new MiruFieldFilter(
                            MiruFieldName.AUTHOR_ID.getFieldName(),
                            ImmutableList.of(new MiruTermId(actorId.toStringForm().getBytes(Charsets.UTF_8)))))),
                        Optional.<ImmutableList<MiruFilter>>absent()))
                    .setAggregateCountAroundField(MiruFieldName.ACTIVITY_PARENT.getFieldName())
                    .setDesiredNumberOfDistincts(1)
                    .build()
            );
            assertNotNull(result);
            if (result.collectedDistincts > 0) {
                LOG.info("Miru reader returned result: {}", result.collectedDistincts);
            } else {
                LOG.info("Miru reader returned zero count for actor: {}", actorId);
            }
        } finally {
            LOG.stopTimer(timerName);
        }
    }

}
