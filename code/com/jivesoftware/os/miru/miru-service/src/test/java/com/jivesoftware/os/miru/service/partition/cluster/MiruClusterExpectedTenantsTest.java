package com.jivesoftware.os.miru.service.partition.cluster;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.plugin.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.service.partition.MiruExpectedTenants;
import com.jivesoftware.os.miru.service.partition.MiruPartitionInfoProvider;
import com.jivesoftware.os.miru.service.partition.MiruRemoteHostedPartition;
import com.jivesoftware.os.miru.service.partition.MiruTenantTopology;
import com.jivesoftware.os.miru.service.partition.MiruTenantTopologyFactory;
import java.util.Arrays;
import java.util.List;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class MiruClusterExpectedTenantsTest {

    private final MiruHost host = new MiruHost("localhost", 49600);
    private final MiruPartitionId partitionId = MiruPartitionId.of(0);
    private final MiruPartitionCoordInfo coordInfo = new MiruPartitionCoordInfo(MiruPartitionState.online, MiruBackingStorage.hybrid);

    private MiruPartitionInfoProvider partitionInfoProvider;
    private MiruExpectedTenants expectedTenants;

    @BeforeMethod
    public void setUp() throws Exception {
        partitionInfoProvider = mock(MiruPartitionInfoProvider.class);
        expectedTenants = mock(MiruExpectedTenants.class);

        MiruTenantTopologyFactory topologyFactory = mock(MiruTenantTopologyFactory.class);
        MiruClusterRegistry clusterRegistry = mock(MiruClusterRegistry.class);

        when(topologyFactory.create(any(MiruTenantId.class))).thenAnswer(topologyFactoryAnswer);
        when(clusterRegistry.getPartitionsForTenant(any(MiruTenantId.class))).thenAnswer(clusterRegistryAnswer);

        expectedTenants = new MiruClusterExpectedTenants(partitionInfoProvider, topologyFactory, clusterRegistry);
    }

    private final Answer<List<MiruPartition>> clusterRegistryAnswer =
        new Answer<List<MiruPartition>>() {
            @Override
            public List<MiruPartition> answer(InvocationOnMock invocation) throws Throwable {
                MiruTenantId tenantId = (MiruTenantId) invocation.getArguments()[0];
                return ImmutableList.of(new MiruPartition(new MiruPartitionCoord(tenantId, partitionId, host), coordInfo));
            }
        };

    private final Answer<MiruTenantTopology<?>> topologyFactoryAnswer = new Answer<MiruTenantTopology<?>>() {
        @Override
        public MiruTenantTopology<?> answer(InvocationOnMock invocation) throws Throwable {
            MiruTenantId tenantId = (MiruTenantId) invocation.getArguments()[0];
            MiruTenantTopology<?> tenantTopology = mock(MiruTenantTopology.class);
            MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, host);
            RequestHelper requestHelper = mock(RequestHelper.class);
            Optional<MiruHostedPartition<?>> optionalPartition = Optional.<MiruHostedPartition<?>>of(
                new MiruRemoteHostedPartition<EWAHCompressedBitmap>(coord, partitionInfoProvider, requestHelper));
            when(tenantTopology.getTenantId()).thenReturn(tenantId);
            when(tenantTopology.getPartition(coord)).thenReturn(optionalPartition);
            return tenantTopology;
        }
    };

    @Test
    public void testExpectedTenants() throws Exception {
        MiruTenantId tenantId1 = new MiruTenantId("tenant1".getBytes());
        MiruPartitionCoord coord1 = new MiruPartitionCoord(tenantId1, partitionId, host);
        MiruTenantId tenantId2 = new MiruTenantId("tenant2".getBytes());
        MiruPartitionCoord coord2 = new MiruPartitionCoord(tenantId2, partitionId, host);
        MiruTenantId tenantId3 = new MiruTenantId("tenant3".getBytes());
        MiruPartitionCoord coord3 = new MiruPartitionCoord(tenantId3, partitionId, host);

        for (int i = 0; i < 3; i++) {
            MiruTenantTopology<?> topology1 = expectedTenants.getTopology(tenantId1);
            assertNotNull(topology1);
            assertTrue(topology1.getPartition(coord1).isPresent());

            // calling getTopology() on an unexpected tenant should invoke partitionInfoProvider once on first discovery, and not again
            verify(partitionInfoProvider).put(eq(coord1), eq(coordInfo));
        }

        expectedTenants.expect(Arrays.asList(tenantId2, tenantId3));

        // the unexpected tenant should invoke partitionInfoProvider for the second time during expect()
        verify(partitionInfoProvider, times(2)).put(eq(coord1), eq(coordInfo));

        // the newly expected tenants should invoke partitionInfoProvider for the first time
        verify(partitionInfoProvider).put(eq(coord2), eq(coordInfo));
        verify(partitionInfoProvider).put(eq(coord3), eq(coordInfo));

        MiruTenantTopology<?> topology2 = expectedTenants.getTopology(tenantId2);
        assertNotNull(topology2);
        assertTrue(topology2.getPartition(coord2).isPresent());

        MiruTenantTopology<?> topology3 = expectedTenants.getTopology(tenantId3);
        assertNotNull(topology3);
        assertTrue(topology3.getPartition(coord3).isPresent());

        // neither 'get' should have invoked partitionInfoProvider again
        verify(partitionInfoProvider).put(eq(coord2), eq(coordInfo));
        verify(partitionInfoProvider).put(eq(coord3), eq(coordInfo));

        expectedTenants.expect(Arrays.asList(tenantId3));

        // tenant2 should have been demoted to temporary
        assertNotNull(expectedTenants.getTopology(tenantId1));
        assertNotNull(expectedTenants.getTopology(tenantId2));
        assertNotNull(expectedTenants.getTopology(tenantId3));

        // the 'expect' should invoke 'put' once each, and the 'gets' should not have invoked 'put', so counts increase by 1
        verify(partitionInfoProvider, times(3)).put(eq(coord1), eq(coordInfo));
        verify(partitionInfoProvider, times(2)).put(eq(coord2), eq(coordInfo));
        verify(partitionInfoProvider, times(2)).put(eq(coord3), eq(coordInfo));
    }
}