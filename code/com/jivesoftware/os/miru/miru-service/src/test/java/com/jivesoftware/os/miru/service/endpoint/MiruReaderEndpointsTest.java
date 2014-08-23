package com.jivesoftware.os.miru.service.endpoint;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruAggregateCountsQueryCriteria;
import com.jivesoftware.os.miru.api.MiruAggregateCountsQueryParams;
import com.jivesoftware.os.miru.api.MiruDistinctCountQueryCriteria;
import com.jivesoftware.os.miru.api.MiruDistinctCountQueryParams;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult;
import com.jivesoftware.os.miru.api.query.result.DistinctCountResult;
import java.util.Random;
import javax.ws.rs.core.Response;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class MiruReaderEndpointsTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private MiruReader miruReader;

    private MiruReaderEndpoints miruReaderEndpoints;

    @BeforeMethod
    public void setUp() {
        this.miruReader = mock(MiruReader.class);
        this.miruReaderEndpoints = new MiruReaderEndpoints(miruReader);
    }

    @Test(dataProvider = "miruFilterDataProvider")
    public void testFilterCustomStream(MiruTenantId tenantId, MiruActorId userIdentity, MiruAggregateCountsQueryCriteria queryCriteria) throws Exception {
        AggregateCountsResult expectedResult = generateRandomAggregateCountsResult(new Random(System.currentTimeMillis()));
        filterSetUp(miruReader.filterCustomStream(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria)),
            expectedResult);

        Response response = miruReaderEndpoints.filterCustomStream(new MiruAggregateCountsQueryParams(tenantId, Optional.of(userIdentity),
            Optional.<MiruAuthzExpression>absent(), queryCriteria));

        verifyFilterResults(response, expectedResult);
        verify(miruReader, times(1)).filterCustomStream(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()),
            eq(queryCriteria));
    }

    @Test(dataProvider = "miruFilterDataProvider")
    public void testFilterEmptyCustomStream(MiruTenantId tenantId, MiruActorId userIdentity, MiruAggregateCountsQueryCriteria queryCriteria)
        throws Exception {
        filterSetUp(miruReader.filterCustomStream(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria)),
            null);

        Response response = miruReaderEndpoints.filterCustomStream(new MiruAggregateCountsQueryParams(tenantId, Optional.of(userIdentity),
            Optional.<MiruAuthzExpression>absent(), queryCriteria));

        verifyFilterResults(response, AggregateCountsResult.EMPTY_RESULTS);
        verify(miruReader, times(1)).filterCustomStream(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()),
            eq(queryCriteria));
    }

    @Test(dataProvider = "miruFilterDataProvider")
    public void testFilterCustomStreamWithException(MiruTenantId tenantId, MiruActorId userIdentity, MiruAggregateCountsQueryCriteria queryCriteria)
        throws Exception {
        filterSetUpWithException(miruReader.filterCustomStream(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()),
            eq(queryCriteria)), new RuntimeException("Fake Error!"));

        Response response = miruReaderEndpoints.filterCustomStream(new MiruAggregateCountsQueryParams(tenantId, Optional.of(userIdentity),
            Optional.<MiruAuthzExpression>absent(), queryCriteria));

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        verify(miruReader, times(1)).filterCustomStream(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()),
            eq(queryCriteria));
    }

    @Test(dataProvider = "miruFilterDataProvider")
    public void testFilterInboxStreamAll(MiruTenantId tenantId, MiruActorId userIdentity, MiruAggregateCountsQueryCriteria queryCriteria) throws Exception {
        AggregateCountsResult expectedResult = generateRandomAggregateCountsResult(new Random(System.currentTimeMillis()));
        filterSetUp(miruReader.filterInboxStreamAll(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria)),
            expectedResult);

        Response response = miruReaderEndpoints.filterInboxStreamAll(new MiruAggregateCountsQueryParams(tenantId, Optional.of(userIdentity),
            Optional.<MiruAuthzExpression>absent(), queryCriteria));

        verifyFilterResults(response, expectedResult);
        verify(miruReader, times(1))
            .filterInboxStreamAll(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria));
    }

    @Test(dataProvider = "miruFilterDataProvider")
    public void testFilterEmptyInboxStreamAll(MiruTenantId tenantId, MiruActorId userIdentity, MiruAggregateCountsQueryCriteria queryCriteria)
        throws Exception {
        filterSetUp(miruReader.filterInboxStreamAll(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria)),
            null);

        Response response = miruReaderEndpoints.filterInboxStreamAll(new MiruAggregateCountsQueryParams(tenantId, Optional.of(userIdentity),
            Optional.<MiruAuthzExpression>absent(), queryCriteria));

        verifyFilterResults(response, AggregateCountsResult.EMPTY_RESULTS);
        verify(miruReader, times(1))
            .filterInboxStreamAll(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria));
    }

    @Test(dataProvider = "miruFilterDataProvider")
    public void testFilterInboxStreamAllWithException(MiruTenantId tenantId, MiruActorId userIdentity, MiruAggregateCountsQueryCriteria queryCriteria)
        throws Exception {
        filterSetUpWithException(
            miruReader.filterInboxStreamAll(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria)),
            new RuntimeException("Fake Error!"));

        Response response = miruReaderEndpoints.filterInboxStreamAll(new MiruAggregateCountsQueryParams(tenantId, Optional.of(userIdentity),
            Optional.<MiruAuthzExpression>absent(), queryCriteria));

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        verify(miruReader, times(1))
            .filterInboxStreamAll(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria));
    }

    @Test(dataProvider = "miruFilterDataProvider")
    public void testFilterInboxStreamUnread(MiruTenantId tenantId, MiruActorId userIdentity, MiruAggregateCountsQueryCriteria queryCriteria) throws Exception {
        AggregateCountsResult expectedResult = generateRandomAggregateCountsResult(new Random(System.currentTimeMillis()));
        filterSetUp(
            miruReader.filterInboxStreamUnread(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria)),
            expectedResult);

        Response response = miruReaderEndpoints.filterInboxStreamUnread(new MiruAggregateCountsQueryParams(tenantId, Optional.of(userIdentity),
            Optional.<MiruAuthzExpression>absent(), queryCriteria));

        verifyFilterResults(response, expectedResult);
        verify(miruReader, times(1))
            .filterInboxStreamUnread(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria));
    }

    @Test(dataProvider = "miruFilterDataProvider")
    public void testFilterEmptyInboxStreamUnread(MiruTenantId tenantId, MiruActorId userIdentity, MiruAggregateCountsQueryCriteria queryCriteria)
        throws Exception {
        filterSetUp(
            miruReader.filterInboxStreamUnread(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria)),
            null);

        Response response = miruReaderEndpoints.filterInboxStreamUnread(new MiruAggregateCountsQueryParams(tenantId, Optional.of(userIdentity),
            Optional.<MiruAuthzExpression>absent(), queryCriteria));

        verifyFilterResults(response, AggregateCountsResult.EMPTY_RESULTS);
        verify(miruReader, times(1))
            .filterInboxStreamUnread(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria));
    }

    @Test(dataProvider = "miruFilterDataProvider")
    public void testFilterInboxStreamUnreadWithException(MiruTenantId tenantId, MiruActorId userIdentity, MiruAggregateCountsQueryCriteria queryCriteria)
        throws Exception {
        filterSetUpWithException(
            miruReader.filterInboxStreamUnread(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria)),
            new RuntimeException("Fake Error!"));

        Response response = miruReaderEndpoints.filterInboxStreamUnread(new MiruAggregateCountsQueryParams(tenantId, Optional.of(userIdentity),
            Optional.<MiruAuthzExpression>absent(), queryCriteria));

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        verify(miruReader, times(1))
            .filterInboxStreamUnread(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria));
    }

    private void filterSetUp(AggregateCountsResult methodCall, AggregateCountsResult returnValue) {
        when(methodCall).thenReturn(returnValue);
    }

    private void filterSetUpWithException(AggregateCountsResult methodCall, RuntimeException exception) {
        when(methodCall).thenThrow(exception);
    }

    private void verifyFilterResults(Response response, AggregateCountsResult expectedResult) throws Exception {
        AggregateCountsResult actualResult = objectMapper.readValue(response.getEntity().toString(), AggregateCountsResult.class);
        assertNotNull(response);
        assertEquals(response.getStatus(), 200);
        assertEquals(actualResult, expectedResult);
    }

    private AggregateCountsResult generateRandomAggregateCountsResult(Random random) {
        ImmutableList.Builder<AggregateCountsResult.AggregateCount> aggregateCountBuilder = ImmutableList.builder();
        for (int i = 0, iterations = random.nextInt(20); i < iterations; i++) {
            byte[] tenantIdBytes = new byte[10];
            random.nextBytes(tenantIdBytes);
            MiruActivity mostRecentActivity = generateRandomMiruActivity(tenantIdBytes);

            byte[] distinctValue = new byte[10];
            random.nextBytes(distinctValue);

            aggregateCountBuilder.add(new AggregateCountsResult.AggregateCount(mostRecentActivity, distinctValue, random.nextLong(), false));
        }

        ImmutableSet.Builder<MiruTermId> aggregateTermBuilder = ImmutableSet.builder();
        for (int i = 0, iterations = random.nextInt(20); i < iterations; i++) {
            byte[] bytes = new byte[10];
            random.nextBytes(bytes);
            aggregateTermBuilder.add(new MiruTermId(bytes));
        }

        return new AggregateCountsResult(aggregateCountBuilder.build(), aggregateTermBuilder.build(), random.nextInt(50), random.nextInt(1000));
    }

    @Test(dataProvider = "miruCountDataProvider")
    public void testCountCustomStream(MiruTenantId tenantId, MiruActorId userIdentity, MiruDistinctCountQueryCriteria queryCriteria) throws Exception {
        DistinctCountResult expectedResult = generateRandomDistinctCountsResult(new Random(System.currentTimeMillis()));
        countSetUp(miruReader.countCustomStream(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria)),
            expectedResult);

        Response response = miruReaderEndpoints
            .countCustomStream(new MiruDistinctCountQueryParams(tenantId, Optional.of(userIdentity), Optional.<MiruAuthzExpression>absent(), queryCriteria));

        verifyCountResults(response, expectedResult);
        verify(miruReader, times(1))
            .countCustomStream(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria));
    }

    @Test(dataProvider = "miruCountDataProvider")
    public void testCountEmptyCustomStream(MiruTenantId tenantId, MiruActorId userIdentity, MiruDistinctCountQueryCriteria queryCriteria)
        throws Exception {
        countSetUp(miruReader.countCustomStream(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria)),
            null);

        Response response = miruReaderEndpoints
            .countCustomStream(new MiruDistinctCountQueryParams(tenantId, Optional.of(userIdentity), Optional.<MiruAuthzExpression>absent(), queryCriteria));

        verifyCountResults(response, DistinctCountResult.EMPTY_RESULTS);
        verify(miruReader, times(1))
            .countCustomStream(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria));
    }

    @Test(dataProvider = "miruCountDataProvider")
    public void testCountCustomStreamWithException(MiruTenantId tenantId, MiruActorId userIdentity, MiruDistinctCountQueryCriteria queryCriteria)
        throws Exception {
        countSetUpWithException(
            miruReader.countCustomStream(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria)),
            new RuntimeException("Fake Error!"));

        Response response = miruReaderEndpoints
            .countCustomStream(new MiruDistinctCountQueryParams(tenantId, Optional.of(userIdentity), Optional.<MiruAuthzExpression>absent(), queryCriteria));

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        verify(miruReader, times(1))
            .countCustomStream(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria));
    }

    @Test(dataProvider = "miruCountDataProvider")
    public void testCountInboxStreamAll(MiruTenantId tenantId, MiruActorId userIdentity, MiruDistinctCountQueryCriteria queryCriteria) throws Exception {
        DistinctCountResult expectedResult = generateRandomDistinctCountsResult(new Random(System.currentTimeMillis()));
        countSetUp(miruReader.countInboxStreamAll(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria)),
            expectedResult);

        Response response = miruReaderEndpoints
            .countInboxStreamAll(new MiruDistinctCountQueryParams(tenantId, Optional.of(userIdentity), Optional.<MiruAuthzExpression>absent(), queryCriteria));

        verifyCountResults(response, expectedResult);
        verify(miruReader, times(1))
            .countInboxStreamAll(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria));
    }

    @Test(dataProvider = "miruCountDataProvider")
    public void testCountEmptyInboxStreamAll(MiruTenantId tenantId, MiruActorId userIdentity, MiruDistinctCountQueryCriteria queryCriteria)
        throws Exception {
        countSetUp(miruReader.countInboxStreamAll(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria)),
            null);

        Response response = miruReaderEndpoints
            .countInboxStreamAll(new MiruDistinctCountQueryParams(tenantId, Optional.of(userIdentity), Optional.<MiruAuthzExpression>absent(), queryCriteria));

        verifyCountResults(response, DistinctCountResult.EMPTY_RESULTS);
        verify(miruReader, times(1))
            .countInboxStreamAll(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria));
    }

    @Test(dataProvider = "miruCountDataProvider")
    public void testCountInboxStreamAllWithException(MiruTenantId tenantId, MiruActorId userIdentity, MiruDistinctCountQueryCriteria queryCriteria)
        throws Exception {
        countSetUpWithException(
            miruReader.countInboxStreamAll(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria)),
            new RuntimeException("Fake Error!"));

        Response response = miruReaderEndpoints
            .countInboxStreamAll(new MiruDistinctCountQueryParams(tenantId, Optional.of(userIdentity), Optional.<MiruAuthzExpression>absent(), queryCriteria));

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        verify(miruReader, times(1))
            .countInboxStreamAll(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria));
    }

    @Test(dataProvider = "miruCountDataProvider")
    public void testCountInboxStreamUnread(MiruTenantId tenantId, MiruActorId userIdentity, MiruDistinctCountQueryCriteria queryCriteria) throws Exception {
        DistinctCountResult expectedResult = generateRandomDistinctCountsResult(new Random(System.currentTimeMillis()));
        countSetUp(
            miruReader.countInboxStreamUnread(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria)),
            expectedResult);

        Response response = miruReaderEndpoints.countInboxStreamUnread(
            new MiruDistinctCountQueryParams(tenantId, Optional.of(userIdentity), Optional.<MiruAuthzExpression>absent(), queryCriteria));

        verifyCountResults(response, expectedResult);
        verify(miruReader, times(1))
            .countInboxStreamUnread(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria));
    }

    @Test(dataProvider = "miruCountDataProvider")
    public void testCountEmptyInboxStreamUnread(MiruTenantId tenantId, MiruActorId userIdentity, MiruDistinctCountQueryCriteria queryCriteria)
        throws Exception {
        countSetUp(
            miruReader.countInboxStreamUnread(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria)),
            null);

        Response response = miruReaderEndpoints.countInboxStreamUnread(
            new MiruDistinctCountQueryParams(tenantId, Optional.of(userIdentity), Optional.<MiruAuthzExpression>absent(), queryCriteria));

        verifyCountResults(response, DistinctCountResult.EMPTY_RESULTS);
        verify(miruReader, times(1))
            .countInboxStreamUnread(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria));
    }

    @Test(dataProvider = "miruCountDataProvider")
    public void testCountInboxStreamUnreadWithException(MiruTenantId tenantId, MiruActorId userIdentity, MiruDistinctCountQueryCriteria queryCriteria)
        throws Exception {
        countSetUpWithException(
            miruReader.countInboxStreamUnread(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria)),
            new RuntimeException("Fake Error!"));

        Response response = miruReaderEndpoints.countInboxStreamUnread(
            new MiruDistinctCountQueryParams(tenantId, Optional.of(userIdentity), Optional.<MiruAuthzExpression>absent(), queryCriteria));

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        verify(miruReader, times(1))
            .countInboxStreamUnread(eq(tenantId), eq(Optional.of(userIdentity)), eq(Optional.<MiruAuthzExpression>absent()), eq(queryCriteria));
    }

    private void countSetUp(DistinctCountResult methodCall, DistinctCountResult returnValue) {
        when(methodCall).thenReturn(returnValue);
    }

    private void countSetUpWithException(DistinctCountResult methodCall, RuntimeException exception) {
        when(methodCall).thenThrow(exception);
    }

    private void verifyCountResults(Response response, DistinctCountResult expectedResult) throws Exception {
        DistinctCountResult actualResult = objectMapper.readValue(response.getEntity().toString(), DistinctCountResult.class);
        assertNotNull(response);
        assertEquals(response.getStatus(), 200);
        assertEquals(actualResult, expectedResult);
    }

    private DistinctCountResult generateRandomDistinctCountsResult(Random random) {
        ImmutableSet.Builder<MiruTermId> aggregateTermBuilder = ImmutableSet.builder();
        for (int i = 0, iterations = random.nextInt(20); i < iterations; i++) {
            byte[] bytes = new byte[10];
            random.nextBytes(bytes);
            aggregateTermBuilder.add(new MiruTermId(bytes));
        }

        return new DistinctCountResult(aggregateTermBuilder.build(), random.nextInt(1000));
    }

    private MiruActivity generateRandomMiruActivity(byte[] tenantIdBytes) {
        return new MiruActivity.Builder(new MiruSchema(new MiruFieldDefinition(0, "f")), new MiruTenantId(tenantIdBytes), System.currentTimeMillis(), new String[] { "aaabbbccc" }, 0)
            .putFieldValue("f", RandomStringUtils.randomAlphabetic(5))
            .build();
    }

    @DataProvider(name = "miruFilterDataProvider")
    public Object[][] miruFilterDataProvider() {
        return new Object[][] {
            { new MiruTenantId("miruTenant".getBytes(Charsets.UTF_8)), new MiruActorId(new Id(12345)), mock(MiruAggregateCountsQueryCriteria.class) }
        };
    }

    @DataProvider(name = "miruCountDataProvider")
    public Object[][] miruCountDataProvider() {
        return new Object[][] {
            { new MiruTenantId("miruTenant".getBytes(Charsets.UTF_8)), new MiruActorId(new Id(12345)), mock(MiruDistinctCountQueryCriteria.class) }
        };
    }
}
