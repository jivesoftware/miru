package com.jivesoftware.os.miru.api.wal;

import com.jivesoftware.os.miru.api.topology.NamedCursor;
import java.util.Arrays;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

/**
 *
 */
public class AmzaSipCursorTest {

    @Test
    public void testComparable() {

        AmzaSipCursor cursor1 = new AmzaSipCursor(Arrays.asList(
            new NamedCursor("00001_4846848290113083095", 374353459262005248L),
            new NamedCursor("00002_5460278797972548316", 374353307965079552L),
            new NamedCursor("00003_5915928173792988065", 374353307960893440L)));

        AmzaSipCursor cursor2 = new AmzaSipCursor(Arrays.asList(
            new NamedCursor("00002_5460278797972548316", 374353307965079552L),
            new NamedCursor("00003_5915928173792988065", 374353307960893440L),
            new NamedCursor("00001_4846848290113083095", 374353307851825152L)));

        assertTrue(cursor1.compareTo(cursor2) > 0);
        assertTrue(cursor2.compareTo(cursor1) < 0);
        assertTrue(cursor1.compareTo(cursor1) == 0);
        assertTrue(cursor2.compareTo(cursor2) == 0);
    }
}