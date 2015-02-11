package com.jivesoftware.os.miru.service.index.delta;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.filer.map.store.api.KeyRange;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.TermIdStream;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * DELTA FORCE
 */
public class MiruDeltaFieldIndex<BM> implements MiruFieldIndex<BM> {

    private final MiruBitmaps<BM> bitmaps;
    private final MiruFieldIndex<BM> backingFieldIndex;
    private final ConcurrentSkipListMap<MiruTermId, MiruDeltaInvertedIndex.Delta<BM>>[] fieldIndexDeltas;

    private static final Comparator<MiruTermId> COMPARATOR = new Comparator<MiruTermId>() {

        private final Comparator<byte[]> lexicographicalComparator = UnsignedBytes.lexicographicalComparator();

        @Override
        public int compare(MiruTermId o1, MiruTermId o2) {
            return lexicographicalComparator.compare(o1.getBytes(), o2.getBytes());
        }
    };

    public MiruDeltaFieldIndex(MiruBitmaps<BM> bitmaps,
        MiruFieldIndex<BM> backingFieldIndex,
        int numFields) {
        this.bitmaps = bitmaps;
        this.backingFieldIndex = backingFieldIndex;
        this.fieldIndexDeltas = new ConcurrentSkipListMap[numFields];
        for (int i = 0; i < numFields; i++) {
            fieldIndexDeltas[i] = new ConcurrentSkipListMap<>(COMPARATOR);
        }
    }

    @Override
    public void append(int fieldId, MiruTermId termId, int... ids) throws Exception {
        getOrAllocate(fieldId, termId).append(ids);
    }

    @Override
    public void set(int fieldId, MiruTermId termId, int... ids) throws Exception {
        getOrAllocate(fieldId, termId).set(ids);
    }

    @Override
    public void remove(int fieldId, MiruTermId termId, int id) throws Exception {
        MiruInvertedIndex<BM> got = get(fieldId, termId);
        got.remove(id);
    }

    @Override
    public void streamTermIdsForField(int fieldId, List<KeyRange> ranges, final TermIdStream termIdStream) throws Exception {
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
        backingFieldIndex.streamTermIdsForField(fieldId, ranges, new TermIdStream() {
            @Override
            public boolean stream(MiruTermId termId) {
                if (termId != null) {
                    if (!indexKeys.contains(termId)) {
                        if (!termIdStream.stream(termId)) {
                            return false;
                        }
                    }
                }
                return true;
            }
        });
    }

    @Override
    public MiruInvertedIndex<BM> get(int fieldId, MiruTermId termId) throws Exception {
        MiruDeltaInvertedIndex.Delta<BM> delta = fieldIndexDeltas[fieldId].get(termId);
        if (delta == null) {
            delta = new MiruDeltaInvertedIndex.Delta<>();
            MiruDeltaInvertedIndex.Delta<BM> existing = fieldIndexDeltas[fieldId].putIfAbsent(termId, delta);
            if (existing != null) {
                delta = existing;
            }
        }
        return new MiruDeltaInvertedIndex<>(bitmaps, backingFieldIndex.get(fieldId, termId), delta);
    }

    @Override
    public MiruInvertedIndex<BM> get(int fieldId, MiruTermId termId, int considerIfIndexIdGreaterThanN) throws Exception {
        MiruDeltaInvertedIndex.Delta<BM> delta = fieldIndexDeltas[fieldId].get(termId);
        if (delta == null) {
            delta = new MiruDeltaInvertedIndex.Delta<>();
            MiruDeltaInvertedIndex.Delta<BM> existing = fieldIndexDeltas[fieldId].putIfAbsent(termId, delta);
            if (existing != null) {
                delta = existing;
            }
        }
        return new MiruDeltaInvertedIndex<>(bitmaps, backingFieldIndex.get(fieldId, termId, considerIfIndexIdGreaterThanN), delta);
    }

    @Override
    public MiruInvertedIndex<BM> getOrCreateInvertedIndex(int fieldId, MiruTermId termId) throws Exception {
        return getOrAllocate(fieldId, termId);
    }

    private MiruInvertedIndex<BM> getOrAllocate(int fieldId, MiruTermId termId) throws Exception {
        MiruDeltaInvertedIndex.Delta<BM> delta = fieldIndexDeltas[fieldId].get(termId);
        if (delta == null) {
            delta = new MiruDeltaInvertedIndex.Delta<>();
            MiruDeltaInvertedIndex.Delta<BM> existing = fieldIndexDeltas[fieldId].putIfAbsent(termId, delta);
            if (existing != null) {
                delta = existing;
            }
        }
        return new MiruDeltaInvertedIndex<>(bitmaps, backingFieldIndex.getOrCreateInvertedIndex(fieldId, termId), delta);
    }

    public void merge() throws Exception {
        for (int fieldId = 0; fieldId < fieldIndexDeltas.length; fieldId++) {
            ConcurrentMap<MiruTermId, MiruDeltaInvertedIndex.Delta<BM>> deltaMap = fieldIndexDeltas[fieldId];
            for (Map.Entry<MiruTermId, MiruDeltaInvertedIndex.Delta<BM>> entry : deltaMap.entrySet()) {
                MiruDeltaInvertedIndex.Delta<BM> delta = entry.getValue();
                MiruDeltaInvertedIndex<BM> invertedIndex = new MiruDeltaInvertedIndex<>(bitmaps,
                    backingFieldIndex.getOrCreateInvertedIndex(fieldId, entry.getKey()),
                    delta);
                invertedIndex.merge();
            }
            deltaMap.clear();
        }
    }
}
