package com.jivesoftware.os.miru.manage.deployable;

import java.util.Arrays;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class CaterpillarSelectHostsStrategyTest {

    @Test
    public void testStartOfContiguousRun() throws Exception {
        CaterpillarSelectHostsStrategy caterpillarSelectHostsStrategy = new CaterpillarSelectHostsStrategy(true, 1, false);

        assertEquals(0, caterpillarSelectHostsStrategy.startOfContiguousRun(10, Arrays.asList(0, 1, 2)));
        assertEquals(0, caterpillarSelectHostsStrategy.startOfContiguousRun(10, Arrays.asList(4, 5, 6)));
        assertEquals(0, caterpillarSelectHostsStrategy.startOfContiguousRun(10, Arrays.asList(7, 8, 9)));
        assertEquals(1, caterpillarSelectHostsStrategy.startOfContiguousRun(10, Arrays.asList(0, 8, 9)));
        assertEquals(2, caterpillarSelectHostsStrategy.startOfContiguousRun(10, Arrays.asList(0, 1, 9)));
        assertEquals(-1, caterpillarSelectHostsStrategy.startOfContiguousRun(10, Arrays.asList(0, 2, 3)));
        assertEquals(-1, caterpillarSelectHostsStrategy.startOfContiguousRun(10, Arrays.asList(0, 3, 4)));
        assertEquals(-1, caterpillarSelectHostsStrategy.startOfContiguousRun(10, Arrays.asList(6, 7, 9)));
        assertEquals(-1, caterpillarSelectHostsStrategy.startOfContiguousRun(10, Arrays.asList(6, 8, 9)));
        assertEquals(-1, caterpillarSelectHostsStrategy.startOfContiguousRun(10, Arrays.asList(1, 8, 9)));
        assertEquals(-1, caterpillarSelectHostsStrategy.startOfContiguousRun(10, Arrays.asList(0, 7, 8)));
        assertEquals(-1, caterpillarSelectHostsStrategy.startOfContiguousRun(10, Arrays.asList(0, 1, 8)));
        assertEquals(-1, caterpillarSelectHostsStrategy.startOfContiguousRun(10, Arrays.asList(1, 2, 9)));
    }
}