package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.map.store.VariableKeySizeFileBackMapStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueStoreException;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.index.MiruField;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.BulkEntry;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.MiruFieldIndexKey;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Short-lived (transient) impl. Term dictionary is mem-mapped. Supports index().
 * Next term id is held in memory.
 */
public class MiruHybridField<BM> implements MiruField<BM>, BulkExport<Iterator<BulkEntry<MiruTermId, MiruFieldIndexKey>>> {

    private static final int[] KEY_SIZE_THRESHOLDS = new int[] { 2, 4, 6, 8, 10, 12, 16, 64, 256, 1_024 }; //TODO make this configurable per field?
    private static final int[] KEY_SIZE_PARTITIONS = new int[] { 1, 2, 2, 6, 8,  6,  2,  1,  1,   1     }; //TODO make this configurable per field?
    private static final int[] KEY_SIZE_TO_PARTITION_COUNT = new int[KEY_SIZE_THRESHOLDS[KEY_SIZE_THRESHOLDS.length - 1] + 1];
    static {
        int maximumKeySize = KEY_SIZE_THRESHOLDS[KEY_SIZE_THRESHOLDS.length - 1];
        for (int i = 0; i <= maximumKeySize; i++) {
            KEY_SIZE_TO_PARTITION_COUNT[i] = getPartitionSize(i);
        }
    }
    private static final int PAYLOAD_SIZE = 8; // 2 ints (MiruFieldIndexKey)

    private final MiruFieldDefinition fieldDefinition;
    private final MiruInMemoryIndex<BM> index;
    private final AtomicInteger nextTermId;
    private final VariableKeySizeFileBackMapStore<MiruTermId, MiruFieldIndexKey> termToIndex;
    private final StripingLocksProvider<MiruTermId> stripingLocks = new StripingLocksProvider<>(96);

    public MiruHybridField(MiruFieldDefinition fieldDefinition, MiruInMemoryIndex<BM> index, String[] mapDirectories, int initialPageCapacity) {
        this.fieldDefinition = fieldDefinition;
        this.index = index;
        this.nextTermId = new AtomicInteger();

        this.termToIndex = new VariableKeySizeFileBackMapStore<MiruTermId, MiruFieldIndexKey>(
            mapDirectories, KEY_SIZE_THRESHOLDS, PAYLOAD_SIZE, initialPageCapacity, 64, null) {

            @Override
            protected int keyLength(MiruTermId key) {
                return key.getBytes().length;
            }

            @Override
            public String keyPartition(MiruTermId key) {
                return String.valueOf(Math.abs(key.hashCode()) % KEY_SIZE_TO_PARTITION_COUNT[keyLength(key)]);
            }

            @Override
            protected Iterable<String> keyPartitions(int keyLength) {
                int partitionCount = KEY_SIZE_TO_PARTITION_COUNT[keyLength];
                List<String> partitions = Lists.newArrayListWithCapacity(partitionCount);
                for (int i = 0; i < partitionCount; i++) {
                    partitions.add(String.valueOf(i));
                }
                return partitions;
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

    private static int getPartitionSize(int keyLength) {
        for (int i = 0; i < KEY_SIZE_THRESHOLDS.length; i++) {
            if (KEY_SIZE_THRESHOLDS[i] >= keyLength) {
                return KEY_SIZE_PARTITIONS[i];
            }
        }
        throw new IndexOutOfBoundsException("Key is too long");
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
        return termToIndex.estimateSizeInBytes();
    }

    @Override
    public void index(MiruTermId term, int... ids) throws Exception {
        MiruFieldIndexKey indexKey = getOrCreateTermId(term);
        index.index(fieldDefinition.fieldId, indexKey.getId(), ids);
        if (indexKey.retain(ids[ids.length - 1])) {
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
        MiruFieldIndexKey id = termToIndex.getUnsafe(term);
        return Optional.fromNullable(id);
    }

    private MiruFieldIndexKey getOrCreateTermId(MiruTermId term) throws KeyValueStoreException {
        MiruFieldIndexKey id = termToIndex.getUnsafe(term);
        if (id == null) {
            synchronized (stripingLocks.lock(term)) { // poor mans put if absent
                id = termToIndex.get(term);
                if (id == null) {
                    termToIndex.add(term, new MiruFieldIndexKey(nextTermId.getAndIncrement()));
                    id = termToIndex.get(term);
                }
            }
        }
        return id;
    }

    @Override
    public Iterator<BulkEntry<MiruTermId, MiruFieldIndexKey>> bulkExport(MiruTenantId tenantId) throws Exception {
        return Iterators.transform(termToIndex.iterator(), BulkEntry.<MiruTermId, MiruFieldIndexKey>fromKeyValueStoreEntry());
    }
}
