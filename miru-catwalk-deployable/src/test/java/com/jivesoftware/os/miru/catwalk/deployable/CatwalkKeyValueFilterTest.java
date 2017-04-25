package com.jivesoftware.os.miru.catwalk.deployable;

import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.catwalk.shared.FeatureScore;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class CatwalkKeyValueFilterTest {
    @Test
    public void testFilter() throws Exception {
        CatwalkKeyValueFilter filter = new CatwalkKeyValueFilter(0.1f, 0);
        byte[] bytes = CatwalkModelService.valueToBytes(true,
            1L,
            2L,
            3,
            Arrays.asList(
                new FeatureScore(new MiruTermId[] { new MiruTermId("1".getBytes()), },
                    new long[] { 1L, 100L, 1000L },
                    1000L,
                    1),
                new FeatureScore(new MiruTermId[] { new MiruTermId("1".getBytes()), },
                    new long[] { 10L, 20L, 30L },
                    1000L,
                    1),
                new FeatureScore(new MiruTermId[] { new MiruTermId("1".getBytes()), },
                    new long[] { 97L, 98L, 99L },
                    1000L,
                    1),
                new FeatureScore(new MiruTermId[] { new MiruTermId("1".getBytes()), },
                    new long[] { 101L, 102L, 103L },
                    1000L,
                    1),
                new FeatureScore(new MiruTermId[] { new MiruTermId("1".getBytes()), },
                    new long[] { 0L, 0L, 0L },
                    1000L,
                    1)),
            new MiruTimeRange(4L, 5L));

        ModelFeatureScores[] result = new ModelFeatureScores[1];
        filter.filter(null, null, bytes, 0L, false, 0L, (prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
            result[0] = CatwalkModelService.valueFromBytes(value, -1);
            return true;
        });

        Assert.assertEquals(result[0].featureScores.size(), 2);
        Assert.assertEquals(result[0].featureScores.get(0).numerators[2], 1000);
        Assert.assertEquals(result[0].featureScores.get(1).numerators[2], 103);
    }

    @Test
    public void testTopNFilter() throws Exception {
        CatwalkKeyValueFilter filter = new CatwalkKeyValueFilter(0, 2);
        byte[] bytes = CatwalkModelService.valueToBytes(true,
            1L,
            2L,
            3,
            Arrays.asList(
                new FeatureScore(new MiruTermId[] { new MiruTermId("1".getBytes()), },
                    new long[] { 1L, 100L, 1000L },
                    1000L,
                    1),
                new FeatureScore(new MiruTermId[] { new MiruTermId("1".getBytes()), },
                    new long[] { 10L, 20L, 30L },
                    1000L,
                    1),
                new FeatureScore(new MiruTermId[] { new MiruTermId("1".getBytes()), },
                    new long[] { 97L, 98L, 99L },
                    1000L,
                    1),
                new FeatureScore(new MiruTermId[] { new MiruTermId("1".getBytes()), },
                    new long[] { 101L, 102L, 103L },
                    1000L,
                    1),
                new FeatureScore(new MiruTermId[] { new MiruTermId("1".getBytes()), },
                    new long[] { 0L, 0L, 0L },
                    1000L,
                    1)),
            new MiruTimeRange(4L, 5L));

        ModelFeatureScores[] result = new ModelFeatureScores[1];
        filter.filter(null, null, bytes, 0L, false, 0L, (prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
            result[0] = CatwalkModelService.valueFromBytes(value, -1);
            return true;
        });

        System.out.println(result[0].featureScores);
        Assert.assertEquals(result[0].featureScores.size(), 2);
        Assert.assertEquals(result[0].featureScores.get(0).numerators[2], 1000);
        Assert.assertEquals(result[0].featureScores.get(1).numerators[2], 103);
    }
}