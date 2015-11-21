package com.jivesoftware.os.miru.service.index.filer;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.map.MapContext;
import com.jivesoftware.os.filer.io.map.MapStore;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.TermIdStream;
import java.util.Arrays;
import java.util.List;

/**
 * @author jonathan
 */
public class MiruFilerFieldIndex<BM extends IBM, IBM> implements MiruFieldIndex<IBM> {

    private final MiruBitmaps<BM, IBM> bitmaps;
    private final long[] indexIds;
    private final KeyedFilerStore<Long, Void>[] indexes;
    private final KeyedFilerStore<Integer, MapContext>[] cardinalities;
    // We could lock on both field + termId for improved hash/striping, but we favor just termId to reduce object creation
    private final StripingLocksProvider<MiruTermId> stripingLocksProvider;

    public MiruFilerFieldIndex(MiruBitmaps<BM, IBM> bitmaps,
        long[] indexIds,
        KeyedFilerStore<Long, Void>[] indexes,
        KeyedFilerStore<Integer, MapContext>[] cardinalities,
        StripingLocksProvider<MiruTermId> stripingLocksProvider) throws Exception {
        this.bitmaps = bitmaps;
        this.indexIds = indexIds;
        this.indexes = indexes;
        this.cardinalities = cardinalities;
        this.stripingLocksProvider = stripingLocksProvider;
    }

    @Override
    public long getVersion(int fieldId, MiruTermId termId) throws Exception {
        return -1;
    }

    @Override
    public void append(int fieldId, MiruTermId termId, int[] ids, long[] counts, byte[] primitiveBuffer) throws Exception {
        getIndex(fieldId, termId, -1).append(primitiveBuffer, ids);
        mergeCardinalities(fieldId, termId, ids, counts, primitiveBuffer);
    }

    @Override
    public void set(int fieldId, MiruTermId termId, int[] ids, long[] counts, byte[] primitiveBuffer) throws Exception {
        getIndex(fieldId, termId, -1).set(primitiveBuffer, ids);
        mergeCardinalities(fieldId, termId, ids, counts, primitiveBuffer);
    }

    @Override
    public void remove(int fieldId, MiruTermId termId, int id, byte[] primitiveBuffer) throws Exception {
        getIndex(fieldId, termId, -1).remove(id, primitiveBuffer);
        mergeCardinalities(fieldId, termId, new int[]{id}, cardinalities[fieldId] != null ? new long[1] : null, primitiveBuffer);
    }

    @Override
    public void streamTermIdsForField(int fieldId, List<KeyRange> ranges, final TermIdStream termIdStream, byte[] primitiveBuffer) throws Exception {
        indexes[fieldId].streamKeys(ranges, iba -> {
            try {
                return termIdStream.stream(new MiruTermId(iba.getBytes()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, primitiveBuffer);
    }

    @Override
    public MiruInvertedIndex<IBM> get(int fieldId, MiruTermId termId) throws Exception {
        return getIndex(fieldId, termId, -1);
    }

    @Override
    public MiruInvertedIndex<IBM> get(int fieldId, MiruTermId termId, int considerIfIndexIdGreaterThanN) throws Exception {
        return getIndex(fieldId, termId, considerIfIndexIdGreaterThanN);
    }

    @Override
    public MiruInvertedIndex<IBM> getOrCreateInvertedIndex(int fieldId, MiruTermId term) throws Exception {
        return getIndex(fieldId, term, -1);
    }

    private MiruInvertedIndex<IBM> getIndex(int fieldId, MiruTermId termId, int considerIfIndexIdGreaterThanN) throws Exception {
        return new MiruFilerInvertedIndex<>(bitmaps, new IndexKey(indexIds[fieldId], termId.getBytes()), indexes[fieldId],
            considerIfIndexIdGreaterThanN, stripingLocksProvider.lock(termId, 0));
    }

    @Override
    public long getCardinality(int fieldId, MiruTermId termId, int id, byte[] primitiveBuffer) throws Exception {
        if (cardinalities[fieldId] != null) {
            Long count = cardinalities[fieldId].read(termId.getBytes(), null, (monkey, filer, _primitiveBuffer, lock) -> {
                if (filer != null) {
                    synchronized (lock) {
                        byte[] payload = MapStore.INSTANCE.getPayload(filer, monkey, FilerIO.intBytes(id), _primitiveBuffer);
                        if (payload != null) {
                            return FilerIO.bytesLong(payload);
                        }
                    }
                }
                return null;
            }, primitiveBuffer);
            return count != null ? count : 0;
        }
        return -1;
    }

    @Override
    public long[] getCardinalities(int fieldId, MiruTermId termId, int[] ids, byte[] primitiveBuffer) throws Exception {
        long[] counts = new long[ids.length];
        if (cardinalities[fieldId] != null) {
            cardinalities[fieldId].read(termId.getBytes(), null, (monkey, filer, _primitiveBuffer, lock) -> {
                if (filer != null) {
                    synchronized (lock) {
                        for (int i = 0; i < ids.length; i++) {
                            if (ids[i] >= 0) {
                                byte[] payload = MapStore.INSTANCE.getPayload(filer, monkey, FilerIO.intBytes(ids[i]), _primitiveBuffer);
                                if (payload != null) {
                                    counts[i] = FilerIO.bytesLong(payload);
                                }
                            }
                        }
                    }
                }
                return null;
            }, primitiveBuffer);
        } else {
            Arrays.fill(counts, -1);
        }
        return counts;
    }

    @Override
    public long getGlobalCardinality(int fieldId, MiruTermId termId, byte[] primitiveBuffer) throws Exception {
        return getCardinality(fieldId, termId, -1, primitiveBuffer);
    }

    @Override
    public void mergeCardinalities(int fieldId, MiruTermId termId, int[] ids, long[] counts, byte[] primitiveBuffer) throws Exception {
        if (cardinalities[fieldId] != null && counts != null) {
            cardinalities[fieldId].readWriteAutoGrow(termId.getBytes(), ids.length, (monkey, filer, _primitiveBuffer, lock) -> {
                synchronized (lock) {
                    long delta = 0;
                    for (int i = 0; i < ids.length; i++) {
                        byte[] key = FilerIO.intBytes(ids[i]);
                        long keyHash = MapStore.INSTANCE.hash(key, 0, key.length);
                        byte[] payload = MapStore.INSTANCE.getPayload(filer, monkey, keyHash, key);
                        long existing = payload != null ? FilerIO.bytesLong(payload) : 0;
                        MapStore.INSTANCE.add(filer, monkey, (byte) 1, keyHash, key, FilerIO.longBytes(counts[i]), _primitiveBuffer);
                        delta += counts[i] - existing;
                    }

                    byte[] globalKey = FilerIO.intBytes(-1);
                    byte[] globalPayload = MapStore.INSTANCE.getPayload(filer, monkey, globalKey, _primitiveBuffer);
                    long globalExisting = globalPayload != null ? FilerIO.bytesLong(globalPayload) : 0;
                    MapStore.INSTANCE.add(filer, monkey, (byte) 1, globalKey, FilerIO.longBytes(globalExisting + delta), _primitiveBuffer);
                }
                return null;
            }, primitiveBuffer);
        }
    }
}
