package com.jivesoftware.os.miru.sea.anomaly.plugins;

import com.google.common.math.LongMath;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.sea.anomaly.plugins.SeaAnomalyAnswer.Waveform;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class SeaAnomaly {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruProvider miruProvider;

    public SeaAnomaly(MiruProvider miruProvider) {
        this.miruProvider = miruProvider;
    }

    public <BM extends IBM, IBM> Waveform metricingSum(MiruBitmaps<BM, IBM> bitmaps,
        BM rawAnswer,
        List<BM> answers,
        int[] indexes,
        int numBits)
        throws Exception {

        log.debug("Get metricing for answers={}", answers);

        long[] waveform = sum(indexes, numBits, answers, bitmaps);

        return new Waveform(waveform);
    }

    public <BM extends IBM, IBM> Waveform metricingAvg(MiruBitmaps<BM, IBM> bitmaps,
        BM rawAnswer,
        List<BM> answers,
        int[] indexes,
        int numBits)
        throws Exception {

        log.debug("Get metricing for answers={}", answers);

        long[] rawCardinalities = new long[indexes.length - 1];
        bitmaps.boundedCardinalities(rawAnswer, indexes, rawCardinalities);

        long[] waveform = sum(indexes, numBits, answers, bitmaps);

        for (int i = 0; i < waveform.length; i++) {
            if (rawCardinalities[i] > 0) {
                waveform[i] /= rawCardinalities[i];
            }
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
    public <BM extends IBM, IBM> Waveform metricingMin(MiruBitmaps<BM, IBM> bitmaps,
        BM rawAnswer,
        List<BM> answers,
        int[] indexes,
        int numBits)
        throws Exception {

        return null; // TODO
    }

    public <BM extends IBM, IBM> Waveform metricingMax(MiruBitmaps<BM, IBM> bitmaps,
        BM rawAnswer,
        List<BM> answers,
        int[] indexes,
        int numBits)
        throws Exception {

        return null; // TODO
    }

    private <BM extends IBM, IBM> long[] sum(int[] indexes, int numBits, List<BM> answers, MiruBitmaps<BM, IBM> bitmaps) {
        long[] waveform = null;
        long[] rawCardinalities = new long[indexes.length - 1];

        for (int i = 0; i < numBits; i++) {
            BM answer = answers.get(i);
            if (answer != null) {
                Arrays.fill(rawCardinalities, 0);
                bitmaps.boundedCardinalities(answer, indexes, rawCardinalities);
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
                            log.inc("overflows");
                        }
                    }
                }
            }
        }
        return waveform;

    }
}
