package com.jivesoftware.os.miru.service.endpoint;

import com.google.common.collect.ImmutableList;
import com.jivesoftware.os.miru.api.MiruWriter;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import java.util.List;
import javax.ws.rs.core.Response;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class MiruWriterEndpointsTest {

    private MiruWriter miruWriter;

    private MiruWriterEndpoints miruWriterEndpoints;

    @BeforeMethod
    public void setUp() {
        this.miruWriter = mock(MiruWriter.class);
        this.miruWriterEndpoints = new MiruWriterEndpoints(miruWriter);
    }

    @Test
    public void testAddActivities() throws Exception {
        MiruPartitionedActivity miruPartitionedActivity1 = mock(MiruPartitionedActivity.class);
        MiruPartitionedActivity miruPartitionedActivity2 = mock(MiruPartitionedActivity.class);
        MiruPartitionedActivity miruPartitionedActivity3 = mock(MiruPartitionedActivity.class);
        List<MiruPartitionedActivity> activities = ImmutableList.of(
            miruPartitionedActivity1,
            miruPartitionedActivity2,
            miruPartitionedActivity3
        );

        Response response = miruWriterEndpoints.addActivities(activities);

        assertNotNull(response);
        assertEquals(response.getStatus(), 200);
        verify(miruWriter, times(1)).writeToIndex(activities);
    }

    @Test
    public void testAddNullActivities() throws Exception {
        Response response = miruWriterEndpoints.addActivities(null);

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        verify(miruWriter, times(0)).writeToIndex(anyListOf(MiruPartitionedActivity.class));
    }

    @Test
    public void testAddActivitiesWithException() throws Exception {
        doThrow(new RuntimeException("Fake Error!")).when(miruWriter).writeToIndex(anyListOf(MiruPartitionedActivity.class));
        Response response = miruWriterEndpoints.addActivities(ImmutableList.<MiruPartitionedActivity>of());

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        verify(miruWriter, times(1)).writeToIndex(anyListOf(MiruPartitionedActivity.class));
    }
}
