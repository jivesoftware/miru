package com.jivesoftware.os.miru.stream.plugins.strut;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.map.MapContext;
import com.jivesoftware.os.filer.io.map.MapStore;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.stream.plugins.strut.Strut.Scored;
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

    void score(String modelId,
        MiruTermId[] termIds,
        KeyedFilerStore<Integer, MapContext>[] cacheStores,
        ScoredStream scoredStream,
        StackBuffer stackBuffer) throws Exception {

        byte[] modelIdBytes = modelId.getBytes(StandardCharsets.UTF_8);

        byte[][][] powerTermIdKeys = new byte[cacheStores.length][][];
        for (int i = 0; i < termIds.length; i++) {
            if (termIds[i] != null) {
                byte[] termIdBytes = termIds[i].getBytes();
                int power = FilerIO.chunkPower(termIdBytes.length, 0);
                if (powerTermIdKeys[power] == null) {
                    powerTermIdKeys[power] = new byte[termIds.length][];
                }
                powerTermIdKeys[power][i] = termIdBytes;
            }
        }

        for (int power = 0; power < powerTermIdKeys.length; power++) {
            long expectedKeySize = FilerIO.chunkLength(power);

            byte[][] termIdKeys = powerTermIdKeys[power];
            if (termIdKeys != null) {
                cacheStores[power].read(modelIdBytes, null, (monkey, filer, _stackBuffer, lock) -> {
                    if (filer != null) {
                        if (monkey.keySize != expectedKeySize) {
                            throw new IllegalStateException("provided " + monkey.keySize + " expected " + expectedKeySize);
                        }
                        synchronized (lock) {
                            for (int i = 0; i < termIdKeys.length; i++) {
                                if (termIdKeys[i] != null) {
                                    byte[] payload = MapStore.INSTANCE.getPayload(filer, monkey, termIdKeys[i], _stackBuffer);
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
                    } else {
                        for (int i = 0; i < termIdKeys.length; i++) {
                            if (termIdKeys[i] != null) {
                                if (!scoredStream.score(i, Float.NaN, -1)) {
                                    return null;
                                }
                            }
                        }
                    }
                    return null;
                }, stackBuffer);
            }
        }
    }

    void commit(String modelId,
        KeyedFilerStore<Integer, MapContext>[] cacheStores,
        List<Strut.Scored> updates,
        StackBuffer stackBuffer) throws Exception {

        byte[] modelIdBytes = modelId.getBytes(StandardCharsets.UTF_8);

        byte[][][] powerTermIdKeys = new byte[cacheStores.length][][];
        for (int i = 0; i < updates.size(); i++) {
            Strut.Scored scored = updates.get(i);
            byte[] termIdBytes = scored.term.getBytes();
            int power = FilerIO.chunkPower(termIdBytes.length, 0);
            if (powerTermIdKeys[power] == null) {
                powerTermIdKeys[power] = new byte[updates.size()][];
            }
            powerTermIdKeys[power][i] = termIdBytes;
        }

        for (int power = 0; power < powerTermIdKeys.length; power++) {
            long expectedKeySize = FilerIO.chunkLength(power);
            byte[][] termIdKeys = powerTermIdKeys[power];
            if (termIdKeys != null) {

                cacheStores[power].readWriteAutoGrow(modelIdBytes,
                    updates.size(), // lazy
                    (MapContext monkey, ChunkFiler chunkFiler, StackBuffer stackBuffer1, Object lock) -> {

                        if (monkey.keySize != expectedKeySize) {
                            throw new IllegalStateException("provided " + monkey.keySize + " expected " + expectedKeySize);
                        }
                        synchronized (lock) {

                            for (int i = 0; i < termIdKeys.length; i++) {
                                if (termIdKeys[i] != null) {

                                    Scored update = updates.get(i);
                                    byte[] scoreBytes = FilerIO.floatBytes(update.score);
                                    byte[] lastId = FilerIO.intBytes(update.lastId);

                                    byte[] payload = new byte[8];
                                    System.arraycopy(scoreBytes, 0, payload, 0, 4);
                                    System.arraycopy(lastId, 0, payload, 4, 4);

                                    MapStore.INSTANCE.add(chunkFiler, monkey, (byte) 1, termIdKeys[i], payload, stackBuffer1);
                                }
                            }
                            return null;
                        }
                    },
                    stackBuffer);
            }
        }
    }
}
