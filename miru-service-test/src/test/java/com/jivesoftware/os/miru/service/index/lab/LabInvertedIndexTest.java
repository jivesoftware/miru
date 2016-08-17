package com.jivesoftware.os.miru.service.index.lab;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 *
 */
public class LabInvertedIndexTest {

    @Test
    public void testAtomizedKeys() throws Exception {
        int key = 23;
        byte[] indexKeyBytes = new byte[] { 0, 1, 2, 3 };
        byte[] atomized = LabInvertedIndex.atomize(indexKeyBytes, key);
        assertNotNull(atomized);
        assertEquals(atomized.length, indexKeyBytes.length + 2);
        int got = LabInvertedIndex.deatomize(atomized);
        assertEquals(got, key);
    }
}
