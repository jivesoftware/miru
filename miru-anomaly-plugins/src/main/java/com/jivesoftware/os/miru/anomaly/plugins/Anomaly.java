package com.jivesoftware.os.miru.anomaly.plugins;

import com.google.common.math.LongMath;
import com.jivesoftware.os.miru.anomaly.plugins.AnomalyAnswer.Waveform;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.math.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math.analysis.polynomials.PolynomialSplineFunction;

/**
 *
 */
public class Anomaly {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final LinearInterpolator interpolator = new LinearInterpolator();

    public <BM extends IBM, IBM> Waveform metricingSum(MiruBitmaps<BM, IBM> bitmaps,
        BM rawAnswer,
        List<BM> answers,
        int[] indexes,
        int numBits)
        throws Exception {

        LOG.debug("Get metricing for answers={}", answers);

        long[] waveform = sum(indexes, numBits, answers, bitmaps);

        return new Waveform(waveform);
    }

    public <BM extends IBM, IBM> Waveform metricingAvg(MiruBitmaps<BM, IBM> bitmaps,
        BM rawAnswer,
        List<BM> answers,
        int[] indexes,
        int numBits)
        throws Exception {

        LOG.debug("Get metricing for answers={}", answers);

        long[] rawCardinalities = new long[indexes.length - 1];
        bitmaps.boundedCardinalities(rawAnswer, new int[][] { indexes }, new long[][] { rawCardinalities });

        long[] waveform = sum(indexes, numBits, answers, bitmaps);

        double[] x = new double[waveform.length];
        double[] y = new double[waveform.length];
        int count = 0;
        int firstIndex = -1;
        int lastIndex = -1;
        for (int i = 0; i < waveform.length; i++) {
            if (rawCardinalities[i] > 0) {
                if (firstIndex == -1) {
                    firstIndex = i;
                }
                lastIndex = i;

                x[count] = i;
                y[count] = waveform[i] / rawCardinalities[i];
                count++;
            }
        }

        if (count == waveform.length) {
            for (int i = 0; i < waveform.length; i++) {
                if (rawCardinalities[i] > 0) {
                    waveform[i] /= rawCardinalities[i];
                }
            }
        } else if (count > 0) {
            double[] ix = new double[count + 2];
            double[] iy = new double[count + 2];
            System.arraycopy(x, 0, ix, 1, count);
            System.arraycopy(y, 0, iy, 1, count);

            ix[0] = -1;
            iy[0] = y[firstIndex];
            ix[ix.length - 1] = x.length;
            iy[iy.length - 1] = y[lastIndex];

            PolynomialSplineFunction splineFunction = interpolator.interpolate(ix, iy);

            long[] interpolated = new long[waveform.length];
            for (int i = 0; i < waveform.length; i++) {
                if (rawCardinalities[i] > 0) {
                    interpolated[i] = waveform[i] / rawCardinalities[i];
                } else {
                    interpolated[i] = (long) splineFunction.value((double) i);
                }
            }

            waveform = interpolated;
        }
        return new Waveform(waveform);
    }


    static <BM extends IBM, IBM> long[] sum(int[] indexes, int numBits, List<BM> answers, MiruBitmaps<BM, IBM> bitmaps) {
        long[] waveform = null;
        long[] rawCardinalities = new long[indexes.length - 1];

        for (int i = 0; i < numBits; i++) {
            BM answer = answers.get(i);
            if (answer != null) {
                Arrays.fill(rawCardinalities, 0);
                bitmaps.boundedCardinalities(answer, new int[][] { indexes }, new long[][] { rawCardinalities });
                if (waveform == null) {
                    waveform = new long[rawCardinalities.length];
                }
                long multiplier = (1L << i);
                for (int j = 0; j < waveform.length; j++) {
                    if (rawCardinalities[j] > 0) {
                        long add = rawCardinalities[j] * multiplier;
                        try {
                            waveform[j] = LongMath.checkedAdd(waveform[j], add);
                        } catch (Exception x) {
                            waveform[j] = Long.MAX_VALUE;
                            LOG.inc("overflows");
                        }
                    }
                }
            }
        }
        return waveform;

    }
}
