package com.jivesoftware.os.miru.stream.plugins.strut;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.map.MapContext;
import com.jivesoftware.os.filer.io.map.MapStore;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class StrutModelScorer {

    public static interface ScoredStream {

        boolean score(int termIndex, float score, int lastId);
    }

    void score(MiruTenantId tenantId,
        String catwalkId,
        String modelId,
        MiruTermId[] termIds,
        KeyedFilerStore<Integer, MapContext> cacheStore,
        ScoredStream scoredStream,
        StackBuffer stackBuffer) throws Exception {

        cacheStore.read((catwalkId + "." + modelId).getBytes(StandardCharsets.UTF_8), null, (monkey, filer, _stackBuffer, lock) -> {

            if (filer != null) {
                synchronized (lock) {
                    for (int i = 0; i < termIds.length; i++) {
                        if (termIds[i] != null) {
                            byte[] payload = MapStore.INSTANCE.getPayload(filer, monkey, termIds[i].getBytes(), _stackBuffer);
                            if (payload != null) {
                                if (!scoredStream.score(i, FilerIO.bytesFloat(payload, 0), FilerIO.bytesInt(payload, 4))) {
                                    return null;
                                }
                            } else if (!scoredStream.score(i, Float.NaN, -1)) {
                                return null;
                            }
                        }
                    }
                }
            }
            return null;
        },
            stackBuffer);
    }

    void commit(MiruTenantId tenantId,
        String catwalkId,
        String modelId,
        KeyedFilerStore<Integer, MapContext> cacheStore,
        List<Strut.Scored> updates,
        StackBuffer stackBuffer) throws Exception {

        cacheStore.readWriteAutoGrow((catwalkId + "." + modelId).getBytes(StandardCharsets.UTF_8),
            updates.size(),
            (MapContext m, ChunkFiler cf, StackBuffer sb1, Object lock) -> {

                synchronized (lock) {
                    byte[] timestampBytes = FilerIO.longBytes(System.currentTimeMillis());
                    byte[] payload = new byte[8];
                    System.arraycopy(timestampBytes, 0, payload, 4, 8);
                    for (Strut.Scored update : updates) {
                        byte[] scoreBytes = FilerIO.floatBytes(update.score);
                        System.arraycopy(scoreBytes, 0, payload, 0, 4);
                        MapStore.INSTANCE.add(cf, m, (byte) 1, update.term.getBytes(), payload, stackBuffer);
                    }
                    return null;
                }
            },
            stackBuffer);
    }
}
