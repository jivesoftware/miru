package com.jivesoftware.os.miru.service.partition.cluster;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.health.api.HealthCheckConfigBinder;
import com.jivesoftware.os.jive.utils.health.api.HealthCheckRegistry;
import com.jivesoftware.os.jive.utils.health.api.HealthChecker;
import com.jivesoftware.os.jive.utils.health.api.HealthFactory;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition;
import com.jivesoftware.os.miru.service.partition.MiruLocalPartitionFactory;
import com.jivesoftware.os.miru.service.partition.MiruRemoteQueryablePartitionFactory;
import java.util.Iterator;
import java.util.Map;
import org.merlin.config.BindInterfaceToConfiguration;
import org.merlin.config.Config;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class MiruTenantTopologyTest {

    private MiruTenantTopology tenantTopology;
    private MiruTenantId tenantId;
    private MiruHost localhost;
    private MiruLocalPartitionFactory localPartitionFactory;
    private MiruRemoteQueryablePartitionFactory remotePartitionFactory;
    private MiruBitmapsEWAH bitmaps;

    @BeforeMethod
    public void setUp() throws Exception {
        HealthFactory.initialize(
            new HealthCheckConfigBinder() {
                @Override
                public <C extends Config> C bindConfig(Class<C> configurationInterfaceClass) {
                    return BindInterfaceToConfiguration.bindDefault(configurationInterfaceClass);
                }
            },
            new HealthCheckRegistry() {
                @Override
                public void register(HealthChecker healthChecker) {
                }

                @Override
                public void unregister(HealthChecker healthChecker) {
                }
            });

        MiruServiceConfig config = mock(MiruServiceConfig.class);
        tenantId = new MiruTenantId("test".getBytes(Charsets.UTF_8));
        localhost = new MiruHost("localhost", 49_600);
        localPartitionFactory = mock(MiruLocalPartitionFactory.class);
        remotePartitionFactory = mock(MiruRemoteQueryablePartitionFactory.class);
        bitmaps = new MiruBitmapsEWAH(2);
        tenantTopology = new MiruTenantTopology<>(config.getEnsurePartitionsIntervalInMillis(), bitmaps, localhost, tenantId, localPartitionFactory);
    }

    @Test
    public void testCheckForPartitionAlignment() throws Exception {
        MiruPartitionId p0 = MiruPartitionId.of(0);
        MiruPartitionId p1 = MiruPartitionId.of(1);

        final Map<MiruPartitionCoord, MiruLocalHostedPartition> coordToPartition = Maps.newHashMap();
        Answer<MiruHostedPartition> localAnswer = new Answer<MiruHostedPartition>() {
            @Override
            public MiruLocalHostedPartition answer(InvocationOnMock invocation) throws Throwable {
                MiruPartitionCoord coord = (MiruPartitionCoord) invocation.getArguments()[1];
                MiruLocalHostedPartition hostedPartition = mock(MiruLocalHostedPartition.class);
                coordToPartition.put(coord, hostedPartition);
                return hostedPartition;
            }
        };

        when(localPartitionFactory.create(same(bitmaps), any(MiruPartitionCoord.class))).thenAnswer(localAnswer);

        tenantTopology.checkForPartitionAlignment(localhost, tenantId, Lists.newArrayList(p0));

        verify(localPartitionFactory).create(same(bitmaps), eq(new MiruPartitionCoord(tenantId, p0, localhost)));
        verifyNoMoreInteractions(localPartitionFactory);

        tenantTopology.checkForPartitionAlignment(localhost, tenantId, Lists.newArrayList(p0, p1));

        verify(localPartitionFactory).create(same(bitmaps), eq(new MiruPartitionCoord(tenantId, p1, localhost)));
        verifyNoMoreInteractions(localPartitionFactory);

        tenantTopology.checkForPartitionAlignment(localhost, tenantId, Lists.newArrayList());

        verifyNoMoreInteractions(localPartitionFactory);
        verify(coordToPartition.get(new MiruPartitionCoord(tenantId, p0, localhost))).remove();
        verify(coordToPartition.get(new MiruPartitionCoord(tenantId, p1, localhost))).remove();
    }

    @Test
    public void testIndexEnsuresPartitions() throws Exception {
        MiruPartitionedActivityFactory factory = new MiruPartitionedActivityFactory();
        MiruPartitionId p0 = MiruPartitionId.of(0);
        MiruPartitionId p1 = MiruPartitionId.of(1);
        MiruPartitionId p2 = MiruPartitionId.of(2);

        Answer<MiruHostedPartition> answer = new Answer<MiruHostedPartition>() {
            @Override
            public MiruHostedPartition answer(InvocationOnMock invocation) throws Throwable {
                final MiruPartitionCoord coord = (MiruPartitionCoord) invocation.getArguments()[1];
                MiruLocalHostedPartition hostedPartition = mock(MiruLocalHostedPartition.class);
                doAnswer(new Answer<Void>() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public Void answer(InvocationOnMock invocation) throws Throwable {
                        Iterator<MiruPartitionedActivity> iter = (Iterator<MiruPartitionedActivity>) invocation.getArguments()[0];
                        while (iter.hasNext()) {
                            MiruPartitionedActivity activity = iter.next();
                            if (activity.getPartitionId() == coord.partitionId.getId()) {
                                iter.remove();
                            }
                        }
                        return null;
                    }
                }).when(hostedPartition).index(Matchers.<Iterator<MiruPartitionedActivity>>any());
                return hostedPartition;
            }
        };
        when(localPartitionFactory.create(same(bitmaps), any(MiruPartitionCoord.class))).thenAnswer(answer);

        tenantTopology.index(Lists.newArrayList(
            factory.activity(0, p0, 0, new MiruActivity.Builder(tenantId, 0, new String[] { "authz" }, 1_111).build()),
            factory.activity(0, p1, 0, new MiruActivity.Builder(tenantId, 1, new String[] { "authz" }, 2_222).build()),
            factory.activity(0, p2, 0, new MiruActivity.Builder(tenantId, 2, new String[] { "authz" }, 3_333).build())));

        verify(localPartitionFactory).create(same(bitmaps), eq(new MiruPartitionCoord(tenantId, p0, localhost)));
        verify(localPartitionFactory).create(same(bitmaps), eq(new MiruPartitionCoord(tenantId, p1, localhost)));
        verify(localPartitionFactory).create(same(bitmaps), eq(new MiruPartitionCoord(tenantId, p2, localhost)));
        verifyNoMoreInteractions(localPartitionFactory);
        verifyZeroInteractions(remotePartitionFactory);
    }
}
