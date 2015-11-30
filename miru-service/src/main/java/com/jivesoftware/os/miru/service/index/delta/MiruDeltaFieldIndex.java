package com.jivesoftware.os.miru.service.index.delta;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.TermIdStream;
import com.jivesoftware.os.miru.service.index.Mergeable;
import gnu.trove.impl.Constants;
import gnu.trove.iterator.TIntLongIterator;
import gnu.trove.map.TIntLongMap;
import gnu.trove.map.hash.TIntLongHashMap;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * DELTA FORCE
 */
public class MiruDeltaFieldIndex<BM extends IBM, IBM> implements MiruFieldIndex<IBM>, Mergeable {

    private final MiruBitmaps<BM, IBM> bitmaps;
    private final MiruFieldIndex<IBM> backingFieldIndex;
    private final ConcurrentSkipListMap<MiruTermId, MiruDeltaInvertedIndex.Delta<IBM>>[] fieldIndexDeltas;
    private final ConcurrentHashMap<MiruTermId, TIntLongMap>[] cardinalities;

    private static final Comparator<MiruTermId> COMPARATOR = new Comparator<MiruTermId>() {

        private final Comparator<byte[]> lexicographicalComparator = UnsignedBytes.lexicographicalComparator();

        @Override
        public int compare(MiruTermId o1, MiruTermId o2) {
            return lexicographicalComparator.compare(o1.getBytes(), o2.getBytes());
        }
    };

    public MiruDeltaFieldIndex(MiruBitmaps<BM, IBM> bitmaps,
        MiruFieldIndex<IBM> backingFieldIndex,
        MiruFieldDefinition[] fieldDefinitions) {
        this.bitmaps = bitmaps;
        this.backingFieldIndex = backingFieldIndex;
        this.fieldIndexDeltas = new ConcurrentSkipListMap[fieldDefinitions.length];
        this.cardinalities = new ConcurrentHashMap[fieldDefinitions.length];
        for (int i = 0; i < fieldDefinitions.length; i++) {
            fieldIndexDeltas[i] = new ConcurrentSkipListMap<>(COMPARATOR);
            if (fieldDefinitions[i].type.hasFeature(MiruFieldDefinition.Feature.cardinality)) {
                cardinalities[i] = new ConcurrentHashMap<>();
            }
        }
    }

    @Override
    public void append(int fieldId, MiruTermId termId, int[] ids, long[] counts, StackBuffer stackBuffer) throws Exception {
        getOrAllocate(fieldId, termId).append(stackBuffer, ids);
        putCardinalities(fieldId, termId, ids, counts, true, stackBuffer);
    }

    @Override
    public void set(int fieldId, MiruTermId termId, int[] ids, long[] counts, StackBuffer stackBuffer) throws Exception {
        getOrAllocate(fieldId, termId).set(stackBuffer, ids);
        putCardinalities(fieldId, termId, ids, counts, false, stackBuffer);
    }

    @Override
    public void remove(int fieldId, MiruTermId termId, int id, StackBuffer stackBuffer) throws Exception {
        MiruInvertedIndex<IBM> got = get(fieldId, termId);
        got.remove(id, stackBuffer);
        putCardinalities(fieldId, termId, new int[] { id }, cardinalities[fieldId] != null ? new long[1] : null, false, stackBuffer);
    }

    @Override
    public void streamTermIdsForField(int fieldId, List<KeyRange> ranges, final TermIdStream termIdStream, StackBuffer stackBuffer) throws Exception {
        final Set<MiruTermId> indexKeys = fieldIndexDeltas[fieldId].keySet();
        if (ranges != null && !ranges.isEmpty()) {
            for (KeyRange range : ranges) {
                final Set<MiruTermId> rangeKeys = fieldIndexDeltas[fieldId].navigableKeySet()
                    .subSet(new MiruTermId(range.getStartInclusiveKey()), new MiruTermId(range.getStopExclusiveKey()));
                for (MiruTermId termId : rangeKeys) {
                    if (!termIdStream.stream(termId)) {
                        return;
                    }
                }
            }
        } else {
            for (MiruTermId termId : indexKeys) {
                if (!termIdStream.stream(termId)) {
                    return;
                }
            }
        }
        backingFieldIndex.streamTermIdsForField(fieldId, ranges, termId -> {
            if (termId != null) {
                if (!indexKeys.contains(termId)) {
                    if (!termIdStream.stream(termId)) {
                        return false;
                    }
                }
            }
            return true;
        }, stackBuffer);
    }

    @Override
    public MiruInvertedIndex<IBM> get(int fieldId, MiruTermId termId) throws Exception {
        MiruDeltaInvertedIndex.Delta<IBM> delta = fieldIndexDeltas[fieldId].get(termId);
        if (delta == null) {
            delta = new MiruDeltaInvertedIndex.Delta<>();
            MiruDeltaInvertedIndex.Delta<IBM> existing = fieldIndexDeltas[fieldId].putIfAbsent(termId, delta);
            if (existing != null) {
                delta = existing;
            }
        }
        return new MiruDeltaInvertedIndex<>(bitmaps, backingFieldIndex.get(fieldId, termId), delta);
    }

    @Override
    public MiruInvertedIndex<IBM> get(int fieldId, MiruTermId termId, int considerIfIndexIdGreaterThanN) throws Exception {
        MiruDeltaInvertedIndex.Delta<IBM> delta = fieldIndexDeltas[fieldId].get(termId);
        if (delta == null) {
            delta = new MiruDeltaInvertedIndex.Delta<>();
            MiruDeltaInvertedIndex.Delta<IBM> existing = fieldIndexDeltas[fieldId].putIfAbsent(termId, delta);
            if (existing != null) {
                delta = existing;
            }
        }
        return new MiruDeltaInvertedIndex<>(bitmaps, backingFieldIndex.get(fieldId, termId, considerIfIndexIdGreaterThanN), delta);
    }

    @Override
    public MiruInvertedIndex<IBM> getOrCreateInvertedIndex(int fieldId, MiruTermId termId) throws Exception {
        return getOrAllocate(fieldId, termId);
    }

    private MiruInvertedIndex<IBM> getOrAllocate(int fieldId, MiruTermId termId) throws Exception {
        MiruDeltaInvertedIndex.Delta<IBM> delta = fieldIndexDeltas[fieldId].get(termId);
        if (delta == null) {
            delta = new MiruDeltaInvertedIndex.Delta<>();
            MiruDeltaInvertedIndex.Delta<IBM> existing = fieldIndexDeltas[fieldId].putIfAbsent(termId, delta);
            if (existing != null) {
                delta = existing;
            }
        }
        return new MiruDeltaInvertedIndex<>(bitmaps, backingFieldIndex.getOrCreateInvertedIndex(fieldId, termId), delta);
    }

    @Override
    public long getCardinality(int fieldId, MiruTermId termId, int id, StackBuffer stackBuffer) throws Exception {
        long[] result = { -1 };
        cardinalities[fieldId].computeIfPresent(termId, (key, idCounts) -> {
            result[0] = idCounts.get(id);
            return idCounts;
        });
        if (result[0] >= 0) {
            return result[0];
        } else {
            return backingFieldIndex.getCardinality(fieldId, termId, id, stackBuffer);
        }
    }

    @Override
    public long[] getCardinalities(int fieldId, MiruTermId termId, int[] ids, StackBuffer stackBuffer) throws Exception {
        long[] counts = new long[ids.length];
        if (cardinalities[fieldId] != null) {
            int[] consumableIds = Arrays.copyOf(ids, ids.length);
            int[] consumed = new int[1];
            cardinalities[fieldId].compute(termId, (key, idCounts) -> {
                if (idCounts != null) {
                    for (int i = 0; i < consumableIds.length; i++) {
                        long count = idCounts.get(consumableIds[i]);
                        counts[i] = count;
                        if (count >= 0) {
                            consumableIds[i] = -1;
                            consumed[0]++;
                        }
                    }
                }
                return idCounts;
            });
            if (consumed[0] < ids.length) {
                long[] existing = backingFieldIndex.getCardinalities(fieldId, termId, consumableIds, stackBuffer);
                for (int i = 0; i < counts.length; i++) {
                    if (counts[i] < 0) {
                        counts[i] = existing[i];
                    }
                }
            }
        } else {
            Arrays.fill(counts, -1);
        }
        return counts;
    }

    @Override
    public long getGlobalCardinality(int fieldId, MiruTermId termId, StackBuffer stackBuffer) throws Exception {
        if (cardinalities[fieldId] != null) {
            long[] count = { -1 };
            cardinalities[fieldId].compute(termId, (key, idCounts) -> {
                if (idCounts != null) {
                    count[0] = idCounts.get(-1);
                }
                return idCounts;
            });
            if (count[0] >= 0) {
                return count[0];
            } else {
                return backingFieldIndex.getGlobalCardinality(fieldId, termId, stackBuffer);
            }
        }
        return -1;
    }

    @Override
    public void mergeCardinalities(int fieldId, MiruTermId termId, int[] ids, long[] counts, StackBuffer stackBuffer) throws Exception {
        putCardinalities(fieldId, termId, ids, counts, false, stackBuffer);
    }

    private void putCardinalities(int fieldId, MiruTermId termId, int[] ids, long[] counts, boolean append, StackBuffer stackBuffer) throws Exception {
        if (cardinalities[fieldId] != null && counts != null) {
            long[] backingCounts = append ? null : backingFieldIndex.getCardinalities(fieldId, termId, ids, stackBuffer);

            cardinalities[fieldId].compute(termId, (key, idCounts) -> {
                if (idCounts == null) {
                    idCounts = new TIntLongHashMap(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1, -1);
                }

                long delta = 0;
                for (int i = 0; i < ids.length; i++) {
                    long existing = idCounts.put(ids[i], counts[i]);
                    if (existing >= 0) {
                        delta = counts[i] - existing;
                    } else if (backingCounts != null && backingCounts[i] > 0) {
                        delta = counts[i] - backingCounts[i];
                    } else {
                        delta = counts[i];
                    }
                }

                if (!idCounts.adjustValue(-1, delta)) {
                    try {
                        long globalCount = backingFieldIndex.getGlobalCardinality(fieldId, termId, stackBuffer);
                        idCounts.put(-1, globalCount > 0 ? delta + globalCount : delta);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                return idCounts;
            });
        }
    }

    @Override
    public void merge(StackBuffer stackBuffer) throws Exception {
        for (int fieldId = 0; fieldId < fieldIndexDeltas.length; fieldId++) {
            ConcurrentMap<MiruTermId, MiruDeltaInvertedIndex.Delta<IBM>> deltaMap = fieldIndexDeltas[fieldId];
            for (Map.Entry<MiruTermId, MiruDeltaInvertedIndex.Delta<IBM>> entry : deltaMap.entrySet()) {
                MiruDeltaInvertedIndex.Delta<IBM> delta = entry.getValue();
                MiruDeltaInvertedIndex<BM, IBM> invertedIndex = new MiruDeltaInvertedIndex<>(bitmaps,
                    backingFieldIndex.getOrCreateInvertedIndex(fieldId, entry.getKey()),
                    delta);
                invertedIndex.merge(stackBuffer);
            }
            deltaMap.clear();

            ConcurrentHashMap<MiruTermId, TIntLongMap> cardinality = cardinalities[fieldId];
            if (cardinality != null) {
                for (Map.Entry<MiruTermId, TIntLongMap> entry : cardinality.entrySet()) {
                    MiruTermId termId = entry.getKey();
                    TIntLongMap idCounts = entry.getValue();
                    int[] ids = new int[idCounts.size() - 1];
                    long[] counts = new long[idCounts.size() - 1];
                    int i = 0;
                    TIntLongIterator iter = idCounts.iterator();
                    while (iter.hasNext()) {
                        iter.advance();
                        int id = iter.key();
                        if (id >= 0) {
                            ids[i] = id;
                            counts[i] = iter.value();
                            i++;
                        }
                    }
                    backingFieldIndex.mergeCardinalities(fieldId, termId, ids, counts, stackBuffer);
                }
                cardinality.clear();
            }
        }
    }
}
