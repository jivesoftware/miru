package com.jivesoftware.os.miru.service.index.filer;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.map.MapContext;
import com.jivesoftware.os.filer.io.map.MapStore;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.TermIdStream;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import java.util.Arrays;
import java.util.List;

/**
 * @author jonathan
 */
public class MiruFilerFieldIndex<BM extends IBM, IBM> implements MiruFieldIndex<IBM> {

    private final MiruBitmaps<BM, IBM> bitmaps;
    private final TrackError trackError;
    private final KeyedFilerStore<Long, Void>[] indexes;
    private final KeyedFilerStore<Integer, MapContext>[] cardinalities;
    // We could lock on both field + termId for improved hash/striping, but we favor just termId to reduce object creation
    private final StripingLocksProvider<MiruTermId> stripingLocksProvider;

    public MiruFilerFieldIndex(MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        KeyedFilerStore<Long, Void>[] indexes,
        KeyedFilerStore<Integer, MapContext>[] cardinalities,
        StripingLocksProvider<MiruTermId> stripingLocksProvider) throws Exception {
        this.bitmaps = bitmaps;
        this.trackError = trackError;
        this.indexes = indexes;
        this.cardinalities = cardinalities;
        this.stripingLocksProvider = stripingLocksProvider;
    }

    @Override
    public void append(int fieldId, MiruTermId termId, int[] ids, long[] counts, StackBuffer stackBuffer) throws Exception {
        getIndex(fieldId, termId, -1).append(stackBuffer, ids);
        mergeCardinalities(fieldId, termId, ids, counts, stackBuffer);
    }

    @Override
    public void set(int fieldId, MiruTermId termId, int[] ids, long[] counts, StackBuffer stackBuffer) throws Exception {
        getIndex(fieldId, termId, -1).set(stackBuffer, ids);
        mergeCardinalities(fieldId, termId, ids, counts, stackBuffer);
    }

    @Override
    public void remove(int fieldId, MiruTermId termId, int id, StackBuffer stackBuffer) throws Exception {
        getIndex(fieldId, termId, -1).remove(id, stackBuffer);
        mergeCardinalities(fieldId, termId, new int[] { id }, cardinalities[fieldId] != null ? new long[1] : null, stackBuffer);
    }

    @Override
    public void streamTermIdsForField(int fieldId, List<KeyRange> ranges, final TermIdStream termIdStream, StackBuffer stackBuffer) throws Exception {
        indexes[fieldId].streamKeys(ranges, rawKey -> {
            try {
                return termIdStream.stream(new MiruTermId(rawKey));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, stackBuffer);
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
        return new MiruFilerInvertedIndex<>(bitmaps, trackError, termId.getBytes(), indexes[fieldId],
            considerIfIndexIdGreaterThanN, stripingLocksProvider.lock(termId, 0));
    }

    @Override
    public long getCardinality(int fieldId, MiruTermId termId, int id, StackBuffer stackBuffer) throws Exception {
        if (cardinalities[fieldId] != null) {
            Long count = cardinalities[fieldId].read(termId.getBytes(), null, (monkey, filer, _stackBuffer, lock) -> {
                if (filer != null) {
                    synchronized (lock) {
                        byte[] payload = MapStore.INSTANCE.getPayload(filer, monkey, FilerIO.intBytes(id), _stackBuffer);
                        if (payload != null) {
                            return FilerIO.bytesLong(payload);
                        }
                    }
                }
                return null;
            }, stackBuffer);
            return count != null ? count : 0;
        }
        return -1;
    }

    @Override
    public long[] getCardinalities(int fieldId, MiruTermId termId, int[] ids, StackBuffer stackBuffer) throws Exception {
        long[] counts = new long[ids.length];
        if (cardinalities[fieldId] != null) {
            cardinalities[fieldId].read(termId.getBytes(), null, (monkey, filer, _stackBuffer, lock) -> {
                if (filer != null) {
                    synchronized (lock) {
                        for (int i = 0; i < ids.length; i++) {
                            if (ids[i] >= 0) {
                                byte[] payload = MapStore.INSTANCE.getPayload(filer, monkey, FilerIO.intBytes(ids[i]), _stackBuffer);
                                if (payload != null) {
                                    counts[i] = FilerIO.bytesLong(payload);
                                }
                            }
                        }
                    }
                }
                return null;
            }, stackBuffer);
        } else {
            Arrays.fill(counts, -1);
        }
        return counts;
    }

    @Override
    public long getGlobalCardinality(int fieldId, MiruTermId termId, StackBuffer stackBuffer) throws Exception {
        return getCardinality(fieldId, termId, -1, stackBuffer);
    }

    @Override
    public void mergeCardinalities(int fieldId, MiruTermId termId, int[] ids, long[] counts, StackBuffer stackBuffer) throws Exception {
        if (cardinalities[fieldId] != null && counts != null) {
            cardinalities[fieldId].readWriteAutoGrow(termId.getBytes(), ids.length, (monkey, filer, _stackBuffer, lock) -> {
                synchronized (lock) {
                    long delta = 0;
                    for (int i = 0; i < ids.length; i++) {
                        byte[] key = FilerIO.intBytes(ids[i]);
                        long keyHash = MapStore.INSTANCE.hash(key, 0, key.length);
                        byte[] payload = MapStore.INSTANCE.getPayload(filer, monkey, keyHash, key, stackBuffer);
                        long existing = payload != null ? FilerIO.bytesLong(payload) : 0;
                        MapStore.INSTANCE.add(filer, monkey, (byte) 1, keyHash, key, FilerIO.longBytes(counts[i]), _stackBuffer);
                        delta += counts[i] - existing;
                    }

                    byte[] globalKey = FilerIO.intBytes(-1);
                    byte[] globalPayload = MapStore.INSTANCE.getPayload(filer, monkey, globalKey, _stackBuffer);
                    long globalExisting = globalPayload != null ? FilerIO.bytesLong(globalPayload) : 0;
                    MapStore.INSTANCE.add(filer, monkey, (byte) 1, globalKey, FilerIO.longBytes(globalExisting + delta), _stackBuffer);
                }
                return null;
            }, stackBuffer);
        }
    }
}
