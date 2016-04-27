package com.jivesoftware.os.miru.stream.plugins.strut;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.CacheKeyValues;
import com.jivesoftware.os.miru.stream.plugins.strut.Strut.Scored;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class StrutModelScorer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public interface ScoredStream {

        boolean score(int termIndex, float[] scores, int lastId);
    }

    void score(String modelId,
        int numeratorsCount,
        MiruTermId[] termIds,
        CacheKeyValues cacheKeyValues,
        ScoredStream scoredStream,
        StackBuffer stackBuffer) throws Exception {

        byte[][] keys = new byte[termIds.length][];
        for (int i = 0; i < keys.length; i++) {
            if (termIds[i] != null) {
                keys[i] = termIds[i].getBytes();
            }
        }
        cacheKeyValues.get(modelId, keys, (index, key, value) -> {
            float[] scores = new float[numeratorsCount];
            int lastId;
            if (value != null && value.length == (4 * numeratorsCount + 4)) {
                int offset = 0;
                for (int i = 0; i < numeratorsCount; i++) {
                    scores[i] = FilerIO.bytesFloat(value, offset);
                    offset += 4;
                }
                lastId = FilerIO.bytesInt(value, offset);
            } else {
                if (value != null) {
                    LOG.warn("Ignored strut model score with invalid length {}", value.length);
                }
                Arrays.fill(scores, Float.NaN);
                lastId = -1;
            }
            return scoredStream.score(index, scores, lastId);
        }, stackBuffer);
    }

    void commit(String modelId,
        CacheKeyValues cacheKeyValues,
        List<Strut.Scored> updates,
        StackBuffer stackBuffer) throws Exception {

        byte[][] keys = new byte[updates.size()][];
        byte[][] values = new byte[updates.size()][];
        for (int i = 0; i < keys.length; i++) {
            Scored update = updates.get(i);
            byte[] payload = new byte[4 * update.scores.length + 4];
            int offset = 0;
            for (int j = 0; j < update.scores.length; j++) {
                byte[] scoreBytes = FilerIO.floatBytes(update.scores[j]);
                System.arraycopy(scoreBytes, 0, payload, offset, 4);
                offset += 4;
            }

            FilerIO.intBytes(update.scoredToLastId, payload, offset);

            keys[i] = update.term.getBytes();
            values[i] = payload;
        }
        cacheKeyValues.put(modelId, keys, values, false, false, stackBuffer);
    }
}
