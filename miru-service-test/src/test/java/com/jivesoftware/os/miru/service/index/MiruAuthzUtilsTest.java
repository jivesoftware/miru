package com.jivesoftware.os.miru.service.index;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.bitmaps.roaring5.buffer.MiruBitmapsRoaringBuffer;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import java.util.List;
import java.util.Map;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 *
 */
public class MiruAuthzUtilsTest {

    Map<String, ImmutableRoaringBitmap> authzIndexes = Maps.newHashMap();
    MiruAuthzUtils<MutableRoaringBitmap, ImmutableRoaringBitmap> utils = new MiruAuthzUtils<>(new MiruBitmapsRoaringBuffer());

    @BeforeMethod
    public void setUp() throws Exception {
        authzIndexes.clear();
        authzIndexes.put("all_ones", bits(0, 72, 1));
        authzIndexes.put("seq_0_2_4_6_8_10", bits(0, 72, 2));
        authzIndexes.put("seq_1_3_5_7_9_11", bits(1, 72, 2));
        authzIndexes.put("seq_0_4_8", bits(0, 72, 4));
        authzIndexes.put("seq_1_5_9", bits(1, 72, 4));
        authzIndexes.put("seq_2_6_10", bits(2, 72, 4));
        authzIndexes.put("seq_3_7_11", bits(3, 72, 4));
    }

    @Test
    public void testCompositeAuthz() throws Exception {

        List<String> values = Lists.newArrayList();
        values.add("seq_0_2_4_6_8_10"); // [0,2,4,6,8,10..]
        values.add("seq_1_3_5_7_9_11"); // [1,3,5,7,9,11..]
        MiruAuthzExpression authzExpression = new MiruAuthzExpression(values);

        ImmutableRoaringBitmap result = utils.getCompositeAuthz(authzExpression, authzIndexes::get);

        // result should be [0,2,4..] | [1,3,5..] = [0,1,2,3..]
        MutableRoaringBitmap expected = BufferFastAggregation.or(authzIndexes.get("seq_0_2_4_6_8_10"), authzIndexes.get("seq_1_3_5_7_9_11"));
        assertEquals(result, expected);
    }

    public static ImmutableRoaringBitmap bits(int start, int end, int... pattern) {
        assertTrue(start >= 0);
        assertTrue(start < end);

        MutableRoaringBitmap bits = new MutableRoaringBitmap();
        bits.add(start);
        int j = 0;
        int last = start;
        for (int i = start + 1; i < end; i++) {
            if (i == last + pattern[j % pattern.length]) {
                bits.add(i);
                last = i;
                j++;
            }
        }
        return bits;
    }
}
