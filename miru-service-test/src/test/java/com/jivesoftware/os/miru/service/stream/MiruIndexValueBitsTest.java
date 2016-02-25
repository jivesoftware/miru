package com.jivesoftware.os.miru.service.stream;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import java.util.Arrays;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

/**
 *
 */
public class MiruIndexValueBitsTest {

    @Test
    public void testComputeDifference() {
        TIntList all = new TIntArrayList(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 });
        TIntList set = new TIntArrayList(new int[] { 1, 3, 5, 7, 9 });
        TIntList diff = MiruIndexValueBits.computeDifference(all, set);

        int[] found = diff.toArray();
        int[] expected = { 0, 2, 4, 6, 8, 10, 11 };
        assertTrue(Arrays.equals(found, expected), "Not equal: " + Arrays.toString(found) + " != " + Arrays.toString(expected));
    }
}
