package com.jivesoftware.os.miru.service.index.memory;

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
public class MiruHybridFieldIndex<BM> implements MiruFieldIndex<BM>,
    BulkImport<Void, BulkStream<BulkExport<Void, BulkStream<BulkEntry<byte[], MiruInvertedIndex<BM>>>>>>,
    BulkExport<Void, BulkStream<BulkExport<Void, BulkStream<BulkEntry<byte[], MiruInvertedIndex<BM>>>>>> {

    private final MiruBitmaps<BM> bitmaps;
    private final KeyValueStore<byte[], ReadWrite<BM>>[] indexes;

    public MiruHybridFieldIndex(MiruBitmaps<BM> bitmaps, KeyValueStore<byte[], ReadWrite<BM>>[] indexes) {
        this.bitmaps = bitmaps;
        this.indexes = indexes;
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
        indexes[fieldId].streamKeys(new KeyValueStore.KeyStream<byte[]>() {
            @Override
            public boolean stream(byte[] bytes) throws IOException {
                return termIdStream.stream(new MiruTermId(bytes));
            }
        });
    }

    @Override
    public MiruInvertedIndex<BM> get(int fieldId, MiruTermId termId) throws Exception {
        return new MiruInMemoryInvertedIndex<>(bitmaps, indexes[fieldId], termId.getBytes(), -1);
    }

    @Override
    public MiruInvertedIndex<BM> get(int fieldId, MiruTermId termId, int considerIfIndexIdGreaterThanN) throws Exception {
        return new MiruInMemoryInvertedIndex<>(bitmaps, indexes[fieldId], termId.getBytes(), considerIfIndexIdGreaterThanN);
    }

    @Override
    public MiruInvertedIndex<BM> getOrCreateInvertedIndex(int fieldId, MiruTermId term) throws Exception {
        return getOrAllocate(fieldId, term);
    }

    private MiruInvertedIndex<BM> getOrAllocate(int fieldId, MiruTermId termId) throws Exception {
        return new MiruInMemoryInvertedIndex<>(bitmaps, indexes[fieldId], termId.getBytes(), -1);
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
                        MiruInMemoryInvertedIndex<BM> invertedIndex = new MiruInMemoryInvertedIndex<>(bitmaps, indexes[fieldId.get()], entry.key, -1);
                        invertedIndex.bulkImport(tenantId, new SimpleBulkExport<>(entry.value));
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

        for (final KeyValueStore<byte[], ReadWrite<BM>> index : indexes) {
            callback.stream(new BulkExport<Void, BulkStream<BulkEntry<byte[], MiruInvertedIndex<BM>>>>() {
                @Override
                public Void bulkExport(MiruTenantId tenantId, final BulkStream<BulkEntry<byte[], MiruInvertedIndex<BM>>> callback) throws Exception {
                    index.stream(new KeyValueStore.EntryStream<byte[], ReadWrite<BM>>() {
                        @Override
                        public boolean stream(byte[] bytes, ReadWrite<BM> readWrite) throws IOException {
                            try {
                                callback.stream(new BulkEntry<byte[], MiruInvertedIndex<BM>>(bytes,
                                    new MiruInMemoryInvertedIndex<>(bitmaps, index, bytes, -1)));
                            } catch (Exception e) {
                                throw new IOException("Failed to stream export", e);
                            }
                            return false;
                        }
                    });
                    return null;
                }
            });
        }
        return null;
    }
}
