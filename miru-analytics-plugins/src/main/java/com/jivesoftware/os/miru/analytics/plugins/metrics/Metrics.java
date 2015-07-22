package com.jivesoftware.os.miru.analytics.plugins.metrics;

import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.solution.Waveform;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;

/**
 *
 */
public class Metrics {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    public <BM> Waveform metricingSum(MiruBitmaps<BM> bitmaps,
        BM rawAnswer,
        List<BM> answers,
        int[] indexes,
        int numBits)
        throws Exception {

        log.debug("Get metricing for answers={}", answers);

        long[] waveform = sum(indexes, numBits, answers, bitmaps);

        return new Waveform(waveform);
    }

    public <BM> Waveform metricingAvg(MiruBitmaps<BM> bitmaps,
        BM rawAnswer,
        List<BM> answers,
        int[] indexes,
        int numBits)
        throws Exception {

        log.debug("Get metricing for answers={}", answers);
        long[] rawCardinalities = bitmaps.boundedCardinalities(rawAnswer, indexes);

        long[] waveform = sum(indexes, numBits, answers, bitmaps);

        for (int i = 0; i < waveform.length; i++) {
            waveform[i] /= rawCardinalities[i];
        }
        return new Waveform(waveform);
    }

    /*
    1,2,3,4,1 avg = avg 4.3, max 4, min 1

    00010 - b2 (card 1)
    01100 - b1 (card 2)
    10101 - b0 (card 3)
    -----
    12341   avg (1+2+3+4+1)/5 max 4, min 1 (cardinality 5)
    */
     public <BM> Waveform metricingMin(MiruBitmaps<BM> bitmaps,
        BM rawAnswer,
        List<BM> answers,
        int[] indexes,
        int numBits)
        throws Exception {

        return null; // TODO
    }

    public <BM> Waveform metricingMax(MiruBitmaps<BM> bitmaps,
        BM rawAnswer,
        List<BM> answers,
        int[] indexes,
        int numBits)
        throws Exception {

        return null; // TODO
    }

    private <BM> long[] sum(int[] indexes, int numBits, List<BM> answers, MiruBitmaps<BM> bitmaps) {
        long[] waveform = new long[indexes.length];
        for (int i = 0; i < numBits; i++) {
            BM answer = answers.get(i);
            if (answer != null) {
                int multiplier = 1 << (numBits - 1 - i);
                long[] cardinalities = bitmaps.boundedCardinalities(answer, indexes);
                for (int j = 0; j < indexes.length; j++) {
                    waveform[j] += multiplier * cardinalities[j];
                }
            }
        }
        return waveform;
    }
}
