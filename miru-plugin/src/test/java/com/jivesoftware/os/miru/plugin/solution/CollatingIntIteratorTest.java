package com.jivesoftware.os.miru.plugin.solution;

import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 *
 */
public class CollatingIntIteratorTest {

    private static MiruIntIterator iter(boolean descending, int... ids) {
        return new MiruIntIterator() {
            private int index = descending ? ids.length - 1 : 0;

            @Override
            public boolean hasNext() {
                return descending ? index >= 0 : index < ids.length;
            }

            @Override
            public int next() {
                return ids[descending ? index-- : index++];
            }
        };
    }

    @Test
    public void testAscending() throws Exception {
        MiruIntIterator[] iterators = new MiruIntIterator[] {
            iter(false, 0, 2, 4, 6, 8, 10),
            iter(false, 1, 3, 5, 7, 9, 11),
            null,
            iter(false, 2, 3, 5, 8, 9, 12),
            iter(false, 3, 4, 5, 6, 7, 13),
        };
        CollatingIntIterator iter = new CollatingIntIterator(iterators, false);

        boolean[] contained = new boolean[iterators.length];

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 0);
        assertEquals(contained, new boolean[] { true, false, false, false, false });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 1);
        assertEquals(contained, new boolean[] { false, true, false, false, false });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 2);
        assertEquals(contained, new boolean[] { true, false, false, true, false });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 3);
        assertEquals(contained, new boolean[] { false, true, false, true, true });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 4);
        assertEquals(contained, new boolean[] { true, false, false, false, true });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 5);
        assertEquals(contained, new boolean[] { false, true, false, true, true });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 6);
        assertEquals(contained, new boolean[] { true, false, false, false, true });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 7);
        assertEquals(contained, new boolean[] { false, true, false, false, true });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 8);
        assertEquals(contained, new boolean[] { true, false, false, true, false });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 9);
        assertEquals(contained, new boolean[] { false, true, false, true, false });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 10);
        assertEquals(contained, new boolean[] { true, false, false, false, false });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 11);
        assertEquals(contained, new boolean[] { false, true, false, false, false });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 12);
        assertEquals(contained, new boolean[] { false, false, false, true, false });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 13);
        assertEquals(contained, new boolean[] { false, false, false, false, true });

        assertFalse(iter.hasNext());
    }

    @Test
    public void testDescending() throws Exception {
        MiruIntIterator[] iterators = new MiruIntIterator[] {
            iter(true, 0, 2, 4, 6, 8),
            iter(true, 1, 3, 5, 7, 9),
            iter(true, 2, 3, 5, 8, 9),
            iter(true, 3, 4, 5, 6, 7),
            null,
        };
        CollatingIntIterator iter = new CollatingIntIterator(iterators, true);

        boolean[] contained = new boolean[iterators.length];

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 9);
        assertEquals(contained, new boolean[] { false, true, true, false, false });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 8);
        assertEquals(contained, new boolean[] { true, false, true, false, false });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 7);
        assertEquals(contained, new boolean[] { false, true, false, true, false });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 6);
        assertEquals(contained, new boolean[] { true, false, false, true, false });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 5);
        assertEquals(contained, new boolean[] { false, true, true, true, false });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 4);
        assertEquals(contained, new boolean[] { true, false, false, true, false });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 3);
        assertEquals(contained, new boolean[] { false, true, true, true, false });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 2);
        assertEquals(contained, new boolean[] { true, false, true, false, false });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 1);
        assertEquals(contained, new boolean[] { false, true, false, false, false });

        assertTrue(iter.hasNext());
        assertEquals(iter.next(contained), 0);
        assertEquals(contained, new boolean[] { true, false, false, false, false });

        assertFalse(iter.hasNext());
    }
}