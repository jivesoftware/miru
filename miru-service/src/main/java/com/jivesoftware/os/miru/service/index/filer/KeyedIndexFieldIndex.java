package com.jivesoftware.os.miru.service.index.filer;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.map.MapStore;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.TermIdStream;
import com.jivesoftware.os.miru.service.stream.KeyedIndex;
import java.util.Arrays;
import java.util.List;

/**
 * @author jonathan
 */
public class KeyedIndexFieldIndex<BM extends IBM, IBM> implements MiruFieldIndex<IBM> {

    private final MiruBitmaps<BM, IBM> bitmaps;
    private final long[] indexIds;
    private final KeyedIndex[] keyedIndexes;
    private final KeyedIndex[] cardinalities;
    // We could lock on both field + termId for improved hash/striping, but we favor just termId to reduce object creation
    private final StripingLocksProvider<MiruTermId> stripingLocksProvider;

    public KeyedIndexFieldIndex(MiruBitmaps<BM, IBM> bitmaps,
        long[] indexIds,
        KeyedIndex[] keyedIndexes,
        KeyedIndex[] cardinalities,
        StripingLocksProvider<MiruTermId> stripingLocksProvider)
        throws Exception {
        this.bitmaps = bitmaps;
        this.indexIds = indexIds;
        this.keyedIndexes = keyedIndexes;
        this.cardinalities = cardinalities;
        this.stripingLocksProvider = stripingLocksProvider;
    }

    @Override
    public long getVersion(int fieldId, MiruTermId termId) throws Exception {
        return -1;
    }

    @Override
    public void append(int fieldId, MiruTermId termId, int[] ids, long[] counts, StackBuffer stackBuffer) throws Exception {
        getOrAllocate(fieldId, termId).append(stackBuffer, ids);
    }

    @Override
    public void set(int fieldId, MiruTermId termId, int[] ids, long[] counts, StackBuffer stackBuffer) throws Exception {
        getOrAllocate(fieldId, termId).set(stackBuffer, ids);
    }

    @Override
    public void remove(int fieldId, MiruTermId termId, int id, StackBuffer stackBuffer) throws Exception {
        MiruInvertedIndex<IBM> got = get(fieldId, termId);
        got.remove(id, stackBuffer);
    }

    @Override
    public void streamTermIdsForField(int fieldId, List<KeyRange> ranges, final TermIdStream termIdStream, StackBuffer stackBuffer) throws Exception {
        keyedIndexes[fieldId].streamKeys(ranges, keyBytes -> termIdStream.stream(new MiruTermId(keyBytes)));
    }

    @Override
    public MiruInvertedIndex<IBM> get(int fieldId, MiruTermId termId) throws Exception {
        return new KeyedIndexInvertedIndex<>(bitmaps, new IndexKey(indexIds[fieldId], termId.getBytes()), keyedIndexes[fieldId], -1,
            stripingLocksProvider.lock(termId, 0));
    }

    @Override
    public MiruInvertedIndex<IBM> get(int fieldId, MiruTermId termId, int considerIfIndexIdGreaterThanN) throws Exception {
        return new KeyedIndexInvertedIndex<>(bitmaps, new IndexKey(indexIds[fieldId], termId.getBytes()), keyedIndexes[fieldId],
            considerIfIndexIdGreaterThanN, stripingLocksProvider.lock(termId, 0));
    }

    @Override
    public MiruInvertedIndex<IBM> getOrCreateInvertedIndex(int fieldId, MiruTermId term) throws Exception {
        return getOrAllocate(fieldId, term);
    }

    private MiruInvertedIndex<IBM> getOrAllocate(int fieldId, MiruTermId termId) throws Exception {
        return new KeyedIndexInvertedIndex<>(bitmaps, new IndexKey(indexIds[fieldId], termId.getBytes()), keyedIndexes[fieldId], -1,
            stripingLocksProvider.lock(termId, 0));
    }

    private byte[] cardinalityKey(MiruTermId termId, int id) {
        byte[] termIdBytes = termId.getBytes();
        byte[] keyBytes = new byte[termIdBytes.length + 4];
        System.arraycopy(termIdBytes, 0, keyBytes, 0, termIdBytes.length);
        FilerIO.intBytes(id, keyBytes, termIdBytes.length);
        return keyBytes;
    }

    @Override
    public long getCardinality(int fieldId, MiruTermId termId, int id, StackBuffer stackBuffer) throws Exception {
        if (cardinalities[fieldId] != null) {
            byte[] value = cardinalities[fieldId].get(cardinalityKey(termId, id));
            return value != null ? FilerIO.bytesLong(value) : 0;
        }
        return -1;
    }

    @Override
    public long[] getCardinalities(int fieldId, MiruTermId termId, int[] ids, StackBuffer stackBuffer) throws Exception {
        long[] counts = new long[ids.length];
        if (cardinalities[fieldId] != null) {
            byte[][] keyBytes = new byte[ids.length][];
            for (int i = 0; i < ids.length; i++) {
                keyBytes[i] = cardinalityKey(termId, ids[i]);
            }
            byte[][] values = cardinalities[fieldId].multiGet(keyBytes);
            for (int i = 0; i < ids.length; i++) {
                counts[i] = values[i] != null ? FilerIO.bytesLong(values[i]) : 0;
            }
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
            long delta = 0;
            for (int i = 0; i < ids.length; i++) {
                byte[] keyBytes = cardinalityKey(termId, ids[i]);
                byte[] payload = cardinalities[fieldId].get(keyBytes);
                cardinalities[fieldId].put(keyBytes, FilerIO.longBytes(counts[i]));
                long existing = payload != null ? FilerIO.bytesLong(payload) : 0;
                delta += counts[i] - existing;
            }

            byte[] globalKey = cardinalityKey(termId, -1);
            byte[] globalPayload = cardinalities[fieldId].get(globalKey);
            long globalExisting = globalPayload != null ? FilerIO.bytesLong(globalPayload) : 0;
            cardinalities[fieldId].put(globalKey, FilerIO.longBytes(globalExisting + delta));
        }
    }
}