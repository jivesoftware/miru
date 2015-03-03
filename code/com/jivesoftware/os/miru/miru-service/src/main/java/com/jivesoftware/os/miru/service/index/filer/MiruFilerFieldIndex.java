package com.jivesoftware.os.miru.service.index.filer;

import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.KeyValueStore;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.TermIdStream;
import java.io.IOException;
import java.util.List;

/**
 * @author jonathan
 */
public class MiruFilerFieldIndex<BM> implements MiruFieldIndex<BM> {

    private final MiruBitmaps<BM> bitmaps;
    private final long[] indexIds;
    private final KeyedFilerStore[] indexes;
    // We could lock on both field + termId for improved hash/striping, but we favor just termId to reduce object creation
    private final StripingLocksProvider<MiruTermId> stripingLocksProvider;

    public MiruFilerFieldIndex(MiruBitmaps<BM> bitmaps,
        long[] indexIds,
        KeyedFilerStore[] indexes,
        StripingLocksProvider<MiruTermId> stripingLocksProvider)
        throws Exception {
        this.bitmaps = bitmaps;
        this.indexIds = indexIds;
        this.indexes = indexes;
        this.stripingLocksProvider = stripingLocksProvider;
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
        indexes[fieldId].streamKeys(ranges, new KeyValueStore.KeyStream<IBA>() {
            @Override
            public boolean stream(IBA iba) throws IOException {
                return termIdStream.stream(new MiruTermId(iba.getBytes()));
            }
        });
    }

    @Override
    public MiruInvertedIndex<BM> get(int fieldId, MiruTermId termId) throws Exception {
        return new MiruFilerInvertedIndex<>(bitmaps, new IndexKey(indexIds[fieldId], termId.getBytes()), indexes[fieldId], -1,
            stripingLocksProvider.lock(termId, 0));
    }

    @Override
    public MiruInvertedIndex<BM> get(int fieldId, MiruTermId termId, int considerIfIndexIdGreaterThanN) throws Exception {
        return new MiruFilerInvertedIndex<>(bitmaps, new IndexKey(indexIds[fieldId], termId.getBytes()), indexes[fieldId],
            considerIfIndexIdGreaterThanN, stripingLocksProvider.lock(termId, 0));
    }

    @Override
    public MiruInvertedIndex<BM> getOrCreateInvertedIndex(int fieldId, MiruTermId term) throws Exception {
        return getOrAllocate(fieldId, term);
    }

    private MiruInvertedIndex<BM> getOrAllocate(int fieldId, MiruTermId termId) throws Exception {
        return new MiruFilerInvertedIndex<>(bitmaps, new IndexKey(indexIds[fieldId], termId.getBytes()), indexes[fieldId], -1,
            stripingLocksProvider.lock(termId, 0));
    }
}
