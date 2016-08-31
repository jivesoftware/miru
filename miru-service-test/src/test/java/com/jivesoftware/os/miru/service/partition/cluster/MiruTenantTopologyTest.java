package com.jivesoftware.os.miru.service.partition.cluster;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruPartitionActiveUpdate;
import com.jivesoftware.os.miru.bitmaps.roaring5.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition;
import com.jivesoftware.os.miru.service.partition.MiruLocalPartitionFactory;
import com.jivesoftware.os.miru.service.partition.MiruRemoteQueryablePartitionFactory;
import com.jivesoftware.os.routing.bird.health.api.HealthCheckRegistry;
import com.jivesoftware.os.routing.bird.health.api.HealthChecker;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import java.util.Iterator;
import java.util.Map;
import org.merlin.config.BindInterfaceToConfiguration;
import org.mockito.Matchers;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
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
    private MiruBitmapsRoaring bitmaps;

    @BeforeMethod
    public void setUp() throws Exception {
        HealthFactory.initialize(
            BindInterfaceToConfiguration::bindDefault,
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
        localhost = new MiruHost("logicalName");
        localPartitionFactory = mock(MiruLocalPartitionFactory.class);
        remotePartitionFactory = mock(MiruRemoteQueryablePartitionFactory.class);
        bitmaps = new MiruBitmapsRoaring();
        tenantTopology = new MiruTenantTopology<>(config.getEnsurePartitionsIntervalInMillis(), bitmaps, localhost, tenantId, localPartitionFactory);
    }

    @Test
    public void testCheckForPartitionAlignment() throws Exception {
        MiruPartitionId p0 = MiruPartitionId.of(0);
        MiruPartitionId p1 = MiruPartitionId.of(1);

        final Map<MiruPartitionCoord, MiruLocalHostedPartition> coordToPartition = Maps.newHashMap();
        Answer<MiruHostedPartition> localAnswer = invocation -> {
            MiruPartitionCoord coord = (MiruPartitionCoord) invocation.getArguments()[1];
            MiruLocalHostedPartition hostedPartition = mock(MiruLocalHostedPartition.class);
            coordToPartition.put(coord, hostedPartition);
            return hostedPartition;
        };

        when(localPartitionFactory.create(same(bitmaps), any(MiruPartitionCoord.class), anyLong())).thenAnswer(localAnswer);

        tenantTopology.checkForPartitionAlignment(Lists.newArrayList(
            new MiruPartitionActiveUpdate(tenantId, p0.getId(), true, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE)));

        verify(localPartitionFactory).create(same(bitmaps), eq(new MiruPartitionCoord(tenantId, p0, localhost)), anyLong());
        verifyNoMoreInteractions(localPartitionFactory);

        tenantTopology.checkForPartitionAlignment(Lists.newArrayList(
            new MiruPartitionActiveUpdate(tenantId, p0.getId(), true, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE),
            new MiruPartitionActiveUpdate(tenantId, p1.getId(), true, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE)));

        verify(localPartitionFactory).create(same(bitmaps), eq(new MiruPartitionCoord(tenantId, p1, localhost)), anyLong());
        verifyNoMoreInteractions(localPartitionFactory);

        tenantTopology.checkForPartitionAlignment(Lists.newArrayList(
            new MiruPartitionActiveUpdate(tenantId, p0.getId(), false, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE),
            new MiruPartitionActiveUpdate(tenantId, p1.getId(), false, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE)));

        verifyNoMoreInteractions(localPartitionFactory);
        verify(coordToPartition.get(new MiruPartitionCoord(tenantId, p0, localhost))).remove();
        verify(coordToPartition.get(new MiruPartitionCoord(tenantId, p1, localhost))).remove();

        tenantTopology.checkForPartitionAlignment(Lists.newArrayList());

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

        Answer<MiruHostedPartition> answer = createInvocation -> {
            final MiruPartitionCoord coord = (MiruPartitionCoord) createInvocation.getArguments()[1];
            MiruLocalHostedPartition hostedPartition = mock(MiruLocalHostedPartition.class);
            doAnswer(indexInvocation -> {
                Iterator<MiruPartitionedActivity> iter = (Iterator<MiruPartitionedActivity>) indexInvocation.getArguments()[0];
                while (iter.hasNext()) {
                    MiruPartitionedActivity activity = iter.next();
                    if (activity.getPartitionId() == coord.partitionId.getId()) {
                        iter.remove();
                    }
                }
                return null;
            }).when(hostedPartition).index(Matchers.<Iterator<MiruPartitionedActivity>>any());
            return hostedPartition;
        };
        when(localPartitionFactory.create(same(bitmaps), any(MiruPartitionCoord.class), anyLong())).thenAnswer(answer);

        tenantTopology.index(Lists.newArrayList(
            factory.activity(0, p0, 0, new MiruActivity.Builder(tenantId, 0, 1_111, false, new String[] { "authz" }).build()),
            factory.activity(0, p1, 0, new MiruActivity.Builder(tenantId, 1, 2_222, false, new String[] { "authz" }).build()),
            factory.activity(0, p2, 0, new MiruActivity.Builder(tenantId, 2, 3_333, false, new String[] { "authz" }).build())));

        verify(localPartitionFactory).create(same(bitmaps), eq(new MiruPartitionCoord(tenantId, p0, localhost)), anyLong());
        verify(localPartitionFactory).create(same(bitmaps), eq(new MiruPartitionCoord(tenantId, p1, localhost)), anyLong());
        verify(localPartitionFactory).create(same(bitmaps), eq(new MiruPartitionCoord(tenantId, p2, localhost)), anyLong());
        verifyNoMoreInteractions(localPartitionFactory);
        verifyZeroInteractions(remotePartitionFactory);
    }
}
