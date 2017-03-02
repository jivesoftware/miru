package com.jivesoftware.os.miru.service.index;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.bitmaps.roaring6.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import java.util.List;
import java.util.Map;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 *
 */
public class MiruAuthzUtilsTest {

    Map<String, RoaringBitmap> authzIndexes = Maps.newHashMap();
    MiruAuthzUtils<RoaringBitmap, RoaringBitmap> utils = new MiruAuthzUtils<>(new MiruBitmapsRoaring());

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

        RoaringBitmap result = utils.getCompositeAuthz(authzExpression, authzIndexes::get);

        // result should be [0,2,4..] | [1,3,5..] = [0,1,2,3..]
        RoaringBitmap expected = FastAggregation.or(authzIndexes.get("seq_0_2_4_6_8_10"), authzIndexes.get("seq_1_3_5_7_9_11"));
        assertEquals(result, expected);
    }

    public static RoaringBitmap bits(int start, int end, int... pattern) {
        assertTrue(start >= 0);
        assertTrue(start < end);

        RoaringBitmap bits = new RoaringBitmap();
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
