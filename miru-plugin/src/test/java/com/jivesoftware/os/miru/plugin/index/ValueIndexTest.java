package com.jivesoftware.os.miru.plugin.index;

import java.nio.charset.StandardCharsets;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 *
 */
public class ValueIndexTest {

    @Test
    public void testPackUnpack() {
        for (String value : new String[] { "", "3 2902", "barf" }) {
            byte[] expected = value.getBytes(StandardCharsets.UTF_8);
            byte[] actual = ValueIndex.unpackValue(ValueIndex.packValue(expected));
            if (expected.length == 0) {
                assertNull(actual);
            } else {
                assertEquals(actual, expected);
            }
        }
    }
}