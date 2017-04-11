package com.jivesoftware.os.miru.stream.plugins.strut;

import com.jivesoftware.os.miru.catwalk.shared.Strategy;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class StrutNGTest {

    public StrutNGTest() {
    }

    @Test
    public void testMax() throws Exception {

        Random r = new Random();
        float max = 0f;
        float[] scores = new float[10];
        int[] counts = new int[10];
        for (int i = 0; i < scores.length; i++) {
            for (int j = 0; j < 100; j++) {
                float s = r.nextFloat();
                max = Math.max(max, s);
                scores[i] = Strut.score(scores[i], s, 1, Strategy.MAX);
                counts[i]++;
            }
        }

        Assert.assertTrue(max == Strut.finalizeScore(scores, counts, Strategy.MAX));
    }

    @Test
    public void testRegessionWeighted() throws Exception {
        float a = Strut.score(0.0f, 0.1f, 1, Strategy.REGRESSION_WEIGHTED);
        float b = Strut.score(0.0f, 0.7f, 1, Strategy.REGRESSION_WEIGHTED);
        float c = Strut.score(0.0f, 0.2f, 1, Strategy.REGRESSION_WEIGHTED);
        float d = Strut.score(0.0f, 0.5f, 1, Strategy.REGRESSION_WEIGHTED);

        float score = Strut.finalizeScore(new float[]{a, b, c, d}, new int[]{1, 1, 1, 1}, Strategy.REGRESSION_WEIGHTED);

        Assert.assertTrue(score == (0.1f + 0.7f + 0.2f + 0.5f));
    }

    @Test
    public void testUnitWeighted() throws Exception {
        float a = Strut.score(0.0f, 0.1f, 1, Strategy.UNIT_WEIGHTED);
        float b = Strut.score(0.0f, 0.7f, 1, Strategy.UNIT_WEIGHTED);
        float c = Strut.score(0.0f, 0.2f, 1, Strategy.UNIT_WEIGHTED);
        float d = Strut.score(0.0f, 0.5f, 1, Strategy.UNIT_WEIGHTED);

        float score = Strut.finalizeScore(new float[]{a, b, c, d}, new int[]{1, 1, 1, 1}, Strategy.UNIT_WEIGHTED);

        Assert.assertTrue(score == (0.1f + 0.7f + 0.2f + 0.5f) / 4);

    }
}
