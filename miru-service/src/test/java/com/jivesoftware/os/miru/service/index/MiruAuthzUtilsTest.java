package com.jivesoftware.os.miru.service.index;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.service.util.BitsTestUtil.ewahBits;
import static org.testng.Assert.assertEquals;

/**
 *
 */
public class MiruAuthzUtilsTest {

    Map<String, EWAHCompressedBitmap> authzIndexes = Maps.newHashMap();
    MiruAuthzUtils<EWAHCompressedBitmap> utils = new MiruAuthzUtils<>(new MiruBitmapsEWAH(4));

    @BeforeMethod
    public void setUp() throws Exception {
        authzIndexes.clear();
        authzIndexes.put("all_ones", ewahBits(0, 72, 1));
        authzIndexes.put("seq_0_2_4_6_8_10", ewahBits(0, 72, 2));
        authzIndexes.put("seq_1_3_5_7_9_11", ewahBits(1, 72, 2));
        authzIndexes.put("seq_0_4_8", ewahBits(0, 72, 4));
        authzIndexes.put("seq_1_5_9", ewahBits(1, 72, 4));
        authzIndexes.put("seq_2_6_10", ewahBits(2, 72, 4));
        authzIndexes.put("seq_3_7_11", ewahBits(3, 72, 4));
    }

    @Test
    public void testCompositeAuthz() throws Exception {

        List<String> values = Lists.newArrayList();
        values.add("seq_0_2_4_6_8_10"); // [0,2,4,6,8,10..]
        values.add("seq_1_3_5_7_9_11"); // [1,3,5,7,9,11..]
        MiruAuthzExpression authzExpression = new MiruAuthzExpression(values);

        EWAHCompressedBitmap result = utils.getCompositeAuthz(authzExpression, authzIndexes::get);

        // result should be [0,2,4..] | [1,3,5..] = [0,1,2,3..]
        EWAHCompressedBitmap expected = authzIndexes.get("seq_0_2_4_6_8_10").or(authzIndexes.get("seq_1_3_5_7_9_11"));
        assertEquals(result, expected);
    }
}
