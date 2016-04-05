package com.jivesoftware.os.miru.stream.plugins.strut;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.CacheKeyValues;
import com.jivesoftware.os.miru.stream.plugins.strut.Strut.Scored;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class StrutModelScorer {

    public interface ScoredStream {

        boolean score(int termIndex, float score, int lastId);
    }

    void score(String modelId,
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
            if (value != null) {
                return scoredStream.score(index, FilerIO.bytesFloat(value, 0), FilerIO.bytesInt(value, 4));
            } else {
                return scoredStream.score(index, Float.NaN, -1);
            }
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
            byte[] scoreBytes = FilerIO.floatBytes(update.score);
            byte[] lastId = FilerIO.intBytes(update.scoredToLastId);

            byte[] payload = new byte[8];
            System.arraycopy(scoreBytes, 0, payload, 0, 4);
            System.arraycopy(lastId, 0, payload, 4, 4);

            keys[i] = update.term.getBytes();
            values[i] = payload;
        }
        cacheKeyValues.put(modelId, keys, values, stackBuffer);
    }
}
