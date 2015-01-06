package com.jivesoftware.os.miru.service.index.disk;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.keyed.store.KeyedFilerStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.TermIdStream;
import com.jivesoftware.os.miru.service.index.BulkEntry;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.BulkStream;
import com.jivesoftware.os.miru.service.index.SimpleBulkExport;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jonathan
 */
public class MiruOnDiskFieldIndex<BM> implements MiruFieldIndex<BM>,
    BulkImport<Void, BulkStream<BulkExport<Void, BulkStream<BulkEntry<byte[], MiruInvertedIndex<BM>>>>>>,
    BulkExport<Void, BulkStream<BulkExport<Void, BulkStream<BulkEntry<byte[], MiruInvertedIndex<BM>>>>>> {

    private final MiruBitmaps<BM> bitmaps;
    private final KeyedFilerStore[] indexes;
    // We could lock on both field + termId for improved hash/striping, but we favor just termId to reduce object creation
    private final StripingLocksProvider<MiruTermId> stripingLocksProvider;

    public MiruOnDiskFieldIndex(MiruBitmaps<BM> bitmaps,
        KeyedFilerStore[] indexes,
        StripingLocksProvider<MiruTermId> stripingLocksProvider)
        throws Exception {
        this.bitmaps = bitmaps;
        this.indexes = indexes;
        this.stripingLocksProvider = stripingLocksProvider;
    }

    @Override
    public long sizeInMemory() throws Exception {
        return 0;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return 0;
    }

    @Override
    public void index(int fieldId, MiruTermId termId, int... ids) throws Exception {
        getOrAllocate(fieldId, termId).append(ids);
    }

    @Override
    public void remove(int fieldId, MiruTermId termId, int id) throws Exception {
        MiruInvertedIndex<BM> got = get(fieldId, termId);
        got.remove(id);
    }

    @Override
    public void streamTermIdsForField(int fieldId, final TermIdStream termIdStream) throws Exception {
        indexes[fieldId].streamKeys(new KeyValueStore.KeyStream<IBA>() {
            @Override
            public boolean stream(IBA iba) throws IOException {
                return termIdStream.stream(new MiruTermId(iba.getBytes()));
            }
        });
    }

    @Override
    public MiruInvertedIndex<BM> get(int fieldId, MiruTermId termId) throws Exception {
        return new MiruOnDiskInvertedIndex<>(bitmaps, indexes[fieldId], termId.getBytes(), -1, stripingLocksProvider.lock(termId));
    }

    @Override
    public MiruInvertedIndex<BM> get(int fieldId, MiruTermId termId, int considerIfIndexIdGreaterThanN) throws Exception {
        return new MiruOnDiskInvertedIndex<>(bitmaps, indexes[fieldId], termId.getBytes(), considerIfIndexIdGreaterThanN,
            stripingLocksProvider.lock(termId));
    }

    @Override
    public MiruInvertedIndex<BM> getOrCreateInvertedIndex(int fieldId, MiruTermId term) throws Exception {
        return getOrAllocate(fieldId, term);
    }

    private MiruInvertedIndex<BM> getOrAllocate(int fieldId, MiruTermId termId) throws Exception {
        return new MiruOnDiskInvertedIndex<>(bitmaps, indexes[fieldId], termId.getBytes(), -1, stripingLocksProvider.lock(termId));
    }

    @Override
    public void bulkImport(final MiruTenantId tenantId,
        BulkExport<Void, BulkStream<BulkExport<Void, BulkStream<BulkEntry<byte[], MiruInvertedIndex<BM>>>>>> export)
        throws Exception {

        final AtomicInteger fieldId = new AtomicInteger(0);
        export.bulkExport(tenantId, new BulkStream<BulkExport<Void, BulkStream<BulkEntry<byte[], MiruInvertedIndex<BM>>>>>() {
            @Override
            public boolean stream(BulkExport<Void, BulkStream<BulkEntry<byte[], MiruInvertedIndex<BM>>>> export) throws Exception {
                export.bulkExport(tenantId, new BulkStream<BulkEntry<byte[], MiruInvertedIndex<BM>>>() {
                    @Override
                    public boolean stream(final BulkEntry<byte[], MiruInvertedIndex<BM>> entry) throws Exception {
                        MiruOnDiskInvertedIndex<BM> miruOnDiskInvertedIndex = new MiruOnDiskInvertedIndex<>(bitmaps, indexes[fieldId.get()], entry.key,
                            -1, stripingLocksProvider.lock(new MiruTermId(entry.key)));
                        miruOnDiskInvertedIndex.bulkImport(tenantId, new SimpleBulkExport<>(entry.value));
                        return true;
                    }
                });
                fieldId.incrementAndGet();
                return true;
            }
        });
    }

    @Override
    public Void bulkExport(MiruTenantId tenantId, BulkStream<BulkExport<Void, BulkStream<BulkEntry<byte[], MiruInvertedIndex<BM>>>>> callback)
        throws Exception {

        for (final KeyedFilerStore index : indexes) {
            if (index == null) {
                continue;
            }
            callback.stream(new BulkExport<Void, BulkStream<BulkEntry<byte[], MiruInvertedIndex<BM>>>>() {
                @Override
                public Void bulkExport(MiruTenantId tenantId, final BulkStream<BulkEntry<byte[], MiruInvertedIndex<BM>>> callback) throws Exception {
                    index.stream(new KeyValueStore.EntryStream<IBA, Filer>() {
                        @Override
                        public boolean stream(IBA iba, Filer filer) throws IOException {
                            try {
                                return callback.stream(new BulkEntry<byte[], MiruInvertedIndex<BM>>(iba.getBytes(),
                                    new MiruOnDiskInvertedIndex<>(bitmaps, index, iba.getBytes(), -1, new Object())));
                            } catch (Exception e) {
                                throw new IOException("Failed to stream export", e);
                            }
                        }
                    });
                    return null;
                }
            });
        }
        return null;
    }
}
