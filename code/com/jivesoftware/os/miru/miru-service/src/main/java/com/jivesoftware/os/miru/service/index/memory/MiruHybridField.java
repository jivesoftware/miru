package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.jivesoftware.os.jive.utils.map.store.VariableKeySizeFileBackMapStore;
import com.jivesoftware.os.jive.utils.map.store.api.KeyValueStoreException;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.index.MiruField;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.BulkEntry;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.MiruFieldIndexKey;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Short-lived (transient) impl. Term dictionary is mem-mapped. Supports index().
 * Next term id is held in memory.
 */
public class MiruHybridField<BM> implements MiruField<BM>, BulkExport<Iterator<BulkEntry<MiruTermId, MiruFieldIndexKey>>> {

    private static final int[] KEY_SIZE_THRESHOLDS = new int[] { 4, 16, 64, 256, 1_024 }; //TODO make this configurable per field?
    private static final int PAYLOAD_SIZE = 8; // 2 ints (MiruFieldIndexKey)

    private final MiruFieldDefinition fieldDefinition;
    private final MiruInMemoryIndex<BM> index;
    private final AtomicInteger nextTermId;
    private final VariableKeySizeFileBackMapStore<MiruTermId, MiruFieldIndexKey> termToIndex;

    public MiruHybridField(MiruFieldDefinition fieldDefinition, MiruInMemoryIndex<BM> index, File mapDirectory) {
        this.fieldDefinition = fieldDefinition;
        this.index = index;
        this.nextTermId = new AtomicInteger();

        String pathToPartitions = mapDirectory.getAbsolutePath();
        this.termToIndex = new VariableKeySizeFileBackMapStore<MiruTermId, MiruFieldIndexKey>(
            pathToPartitions, KEY_SIZE_THRESHOLDS, PAYLOAD_SIZE, 100, 8, null) {

            @Override
            protected int keyLength(MiruTermId key) {
                return key.getBytes().length;
            }

            @Override
            public String keyPartition(MiruTermId key) {
                return "0";
            }

            @Override
            public Iterable<String> keyPartitions() {
                return Collections.singletonList("0");
            }

            @Override
            public byte[] keyBytes(MiruTermId key) {
                return key.getBytes();
            }

            @Override
            public byte[] valueBytes(MiruFieldIndexKey value) {
                ByteBuffer buf = ByteBuffer.allocate(8); // 2 ints
                buf.putInt(value.getId());
                buf.putInt(value.getMaxId());
                return buf.array();
            }

            @Override
            public MiruTermId bytesKey(byte[] bytes, int offset) {
                if (offset != 0) {
                    throw new UnsupportedOperationException("offset not supported");
                }
                return new MiruTermId(bytes);
            }

            @Override
            public MiruFieldIndexKey bytesValue(MiruTermId key, byte[] bytes, int offset) {
                ByteBuffer buf = ByteBuffer.wrap(bytes, offset, 8); // 2 ints
                return new MiruFieldIndexKey(buf.getInt(), buf.getInt());
            }
        };
    }

    @Override
    public MiruFieldDefinition getFieldDefinition() {
        return fieldDefinition;
    }

    @Override
    public long sizeInMemory() throws Exception {
        return 0;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return termToIndex.sizeInBytes();
    }

    @Override
    public void index(MiruTermId term, int id) throws Exception {
        MiruFieldIndexKey indexKey = getOrCreateTermId(term);
        index.index(fieldDefinition.fieldId, indexKey.getId(), id);
        if (indexKey.retain(id)) {
            termToIndex.add(term, indexKey);
        }
    }

    @Override
    public void remove(MiruTermId term, int id) throws Exception {
        MiruFieldIndexKey indexKey = getOrCreateTermId(term);
        index.remove(fieldDefinition.fieldId, indexKey.getId(), id);
    }

    @Override
    public MiruInvertedIndex<BM> getOrCreateInvertedIndex(MiruTermId term) throws Exception {
        MiruFieldIndexKey indexKey = getOrCreateTermId(term);
        Optional<MiruInvertedIndex<BM>> invertedIndex = getInvertedIndex(indexKey);
        if (invertedIndex.isPresent()) {
            return invertedIndex.get();
        }
        return index.allocate(fieldDefinition.fieldId, indexKey.getId());
    }

    @Override
    public Optional<MiruInvertedIndex<BM>> getInvertedIndex(MiruTermId term, int considerIfIndexIdGreaterThanN) throws Exception {
        Optional<MiruFieldIndexKey> indexKey = getTermId(term);
        if (indexKey.isPresent() && indexKey.get().getMaxId() > considerIfIndexIdGreaterThanN) {
            return getInvertedIndex(term);
        }
        return Optional.absent();
    }

    @Override
    public Optional<MiruInvertedIndex<BM>> getInvertedIndex(MiruTermId term) throws Exception {
        Optional<MiruFieldIndexKey> indexKey = getTermId(term);
        if (indexKey.isPresent()) {
            return getInvertedIndex(indexKey.get());
        }
        return Optional.absent();
    }

    public MiruInMemoryIndex getIndex() {
        return index;
    }

    private Optional<MiruInvertedIndex<BM>> getInvertedIndex(MiruFieldIndexKey indexKey) throws Exception {
        if (indexKey != null) {
            return index.get(fieldDefinition.fieldId, indexKey.getId());
        }
        return Optional.absent();
    }

    private Optional<MiruFieldIndexKey> getTermId(MiruTermId term) throws KeyValueStoreException {
        MiruFieldIndexKey id = termToIndex.get(term);
        return Optional.fromNullable(id);
    }

    private MiruFieldIndexKey getOrCreateTermId(MiruTermId term) throws KeyValueStoreException {
        MiruFieldIndexKey id = termToIndex.get(term);
        if (id == null) {
            termToIndex.add(term, new MiruFieldIndexKey(nextTermId.getAndIncrement()));
            id = termToIndex.get(term);
        }
        return id;
    }

    @Override
    public Iterator<BulkEntry<MiruTermId, MiruFieldIndexKey>> bulkExport(MiruTenantId tenantId) throws Exception {
        return Iterators.transform(termToIndex.iterator(), BulkEntry.<MiruTermId, MiruFieldIndexKey>fromKeyValueStoreEntry());
    }
}
