package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.KeyPartitioner;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.map.store.ByteBufferFactoryBackedMapChunkFactory;
import com.jivesoftware.os.filer.map.store.FileBackedMapChunkFactory;
import com.jivesoftware.os.filer.map.store.MapChunkFactory;
import com.jivesoftware.os.filer.map.store.VariableKeySizeMapChunkBackedMapStore;
import com.jivesoftware.os.filer.map.store.api.Copyable;
import com.jivesoftware.os.filer.map.store.api.KeyValueStoreException;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruPartitionState;
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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Short-lived (transient) impl. Term dictionary is mem-mapped. Supports index().
 * Next term id is held in memory.
 */
public class MiruHybridField<BM> implements MiruField<BM>, BulkExport<Iterator<BulkEntry<MiruTermId, MiruFieldIndexKey>>> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final int[] KEY_SIZE_THRESHOLDS = new int[] { 2, 4, 6, 8, 10, 12, 16, 64, 256, 1_024 }; //TODO make this configurable per field?
    private static final int[] KEY_SIZE_PARTITIONS = new int[] { 1, 2, 2, 6, 8, 6, 2, 1, 1, 1 }; //TODO make this configurable per field?

    private static final int PAYLOAD_SIZE = 8; // 2 ints (MiruFieldIndexKey)

    private final MiruFieldDefinition fieldDefinition;
    private final MiruInMemoryIndex<BM> index;
    private final AtomicInteger nextTermId;
    private final HybridIndex<VariableKeySizeMapChunkBackedMapStore<MiruTermId, MiruFieldIndexKey>> hybridIndex;
    private final StripingLocksProvider<MiruTermId> stripingLocks = new StripingLocksProvider<>(96);

    public MiruHybridField(MiruFieldDefinition fieldDefinition,
        MiruInMemoryIndex<BM> index,
        final ByteBufferFactory byteBufferFactory,
        final String[] basePathsToPartitions,
        final int initialPageCapacity)
        throws Exception {

        this.fieldDefinition = fieldDefinition;
        this.index = index;
        this.nextTermId = new AtomicInteger();

        VariableKeySizeMapChunkBackedMapStore<MiruTermId, MiruFieldIndexKey> primaryStore = buildTermToIndex(new KeySizeMapChunkFactoryProvider() {
            @Override
            public MapChunkFactory getFactory(int keySize) {
                return new ByteBufferFactoryBackedMapChunkFactory(keySize, true, PAYLOAD_SIZE, false, initialPageCapacity, byteBufferFactory);
            }
        });

        VariableKeySizeMapChunkBackedMapStore<MiruTermId, MiruFieldIndexKey> migratedStore = null;
        if (basePathsToPartitions != null) {
            migratedStore = buildTermToIndex(new KeySizeMapChunkFactoryProvider() {
                @Override
                public MapChunkFactory getFactory(int keySize) {
                    String[] pathsToPartitions = new String[basePathsToPartitions.length];
                    for (int basePathIndex = 0; basePathIndex < basePathsToPartitions.length; basePathIndex++) {
                        pathsToPartitions[basePathIndex] = new File(basePathsToPartitions[basePathIndex], String.valueOf(keySize)).getAbsolutePath();
                    }
                    return new FileBackedMapChunkFactory(keySize, true, PAYLOAD_SIZE, false, initialPageCapacity, pathsToPartitions);
                }
            });
        }

        this.hybridIndex = new HybridIndex<>(fieldDefinition.name, primaryStore, migratedStore);
    }

    private interface KeySizeMapChunkFactoryProvider {
        MapChunkFactory getFactory(int keySize);
    }

    private static class HybridIndex<V extends Copyable<V, ?>> {

        private static final int PERMITS = 64;

        private final String name;
        private final Semaphore semaphore = new Semaphore(PERMITS);

        private V primaryStore;
        private V secondaryStore;

        private HybridIndex(String name, V primaryStore, V secondaryStore) {
            this.name = name;
            this.primaryStore = primaryStore;
            this.secondaryStore = secondaryStore;
        }

        public V acquire() throws InterruptedException {
            if (primaryStore != null) {
                semaphore.acquire();
                return primaryStore;
            } else {
                return secondaryStore;
            }
        }

        public void release(V store) {
            if (store != secondaryStore) {
                semaphore.release();
            }
        }

        public void migrate() throws Exception {
            if (primaryStore == null || secondaryStore == null) {
                return;
            }
            semaphore.acquire(PERMITS);
            try {
                primaryStore.copyTo(secondaryStore);
                LOG.debug("Migrated {}", name);
                primaryStore = null;
            } finally {
                semaphore.release(PERMITS);
            }
        }
    }

    private VariableKeySizeMapChunkBackedMapStore<MiruTermId, MiruFieldIndexKey> buildTermToIndex(KeySizeMapChunkFactoryProvider factoryProvider)
        throws Exception {

        VariableKeySizeMapChunkBackedMapStore.Builder<MiruTermId, MiruFieldIndexKey> mapStoreBuilder = new VariableKeySizeMapChunkBackedMapStore.Builder<>(
            64,
            null,
            new KeyValueMarshaller<MiruTermId, MiruFieldIndexKey>() {

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
            });

        for (int i = 0; i < KEY_SIZE_THRESHOLDS.length; i++) {
            final int keySize = KEY_SIZE_THRESHOLDS[i];
            final int partitionCount = KEY_SIZE_PARTITIONS[i];
            mapStoreBuilder.add(keySize,
                factoryProvider.getFactory(keySize),
                new KeyPartitioner<MiruTermId>() {
                    @Override
                    public String keyPartition(MiruTermId key) {
                        return String.valueOf(Math.abs(key.hashCode()) % partitionCount);
                    }

                    @Override
                    public Iterable<String> allPartitions() {
                        List<String> partitions = Lists.newArrayListWithCapacity(partitionCount);
                        for (int i = 0; i < partitionCount; i++) {
                            partitions.add(String.valueOf(i));
                        }
                        return partitions;
                    }
                });
        }

        return mapStoreBuilder.build();
    }

    @Override
    public MiruFieldDefinition getFieldDefinition() {
        return fieldDefinition;
    }

    @Override
    public void notifyStateChange(MiruPartitionState state) throws Exception {
        if (state == MiruPartitionState.online) {
            hybridIndex.migrate();
        }
    }

    @Override
    public long sizeInMemory() throws Exception {
        return 0;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        VariableKeySizeMapChunkBackedMapStore<MiruTermId, MiruFieldIndexKey> termToIndex = hybridIndex.acquire();
        try {
            return termToIndex.estimateSizeInBytes();
        } finally {
            hybridIndex.release(termToIndex);
        }
    }

    @Override
    public void index(MiruTermId term, int... ids) throws Exception {
        VariableKeySizeMapChunkBackedMapStore<MiruTermId, MiruFieldIndexKey> termToIndex = hybridIndex.acquire();
        try {
            MiruFieldIndexKey indexKey = getOrCreateTermId(termToIndex, term);
            index.index(fieldDefinition.fieldId, indexKey.getId(), ids);
            if (indexKey.retain(ids[ids.length - 1])) {
                termToIndex.add(term, indexKey);
            }
        } finally {
            hybridIndex.release(termToIndex);
        }
    }

    @Override
    public void remove(MiruTermId term, int id) throws Exception {
        VariableKeySizeMapChunkBackedMapStore<MiruTermId, MiruFieldIndexKey> termToIndex = hybridIndex.acquire();
        try {
            MiruFieldIndexKey indexKey = getOrCreateTermId(termToIndex, term);
            index.remove(fieldDefinition.fieldId, indexKey.getId(), id);
        } finally {
            hybridIndex.release(termToIndex);
        }
    }

    @Override
    public MiruInvertedIndex<BM> getOrCreateInvertedIndex(MiruTermId term) throws Exception {
        VariableKeySizeMapChunkBackedMapStore<MiruTermId, MiruFieldIndexKey> termToIndex = hybridIndex.acquire();
        try {
            MiruFieldIndexKey indexKey = getOrCreateTermId(termToIndex, term);
            Optional<MiruInvertedIndex<BM>> invertedIndex = getInvertedIndex(indexKey);
            if (invertedIndex.isPresent()) {
                return invertedIndex.get();
            }
            return index.allocate(fieldDefinition.fieldId, indexKey.getId());
        } finally {
            hybridIndex.release(termToIndex);
        }
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

    private Optional<MiruFieldIndexKey> getTermId(MiruTermId term) throws Exception {
        VariableKeySizeMapChunkBackedMapStore<MiruTermId, MiruFieldIndexKey> termToIndex = hybridIndex.acquire();
        try {
            MiruFieldIndexKey id = termToIndex.getUnsafe(term);
            return Optional.fromNullable(id);
        } finally {
            hybridIndex.release(termToIndex);
        }
    }

    private MiruFieldIndexKey getOrCreateTermId(VariableKeySizeMapChunkBackedMapStore<MiruTermId, MiruFieldIndexKey> termToIndex, MiruTermId term)
        throws KeyValueStoreException {

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
        VariableKeySizeMapChunkBackedMapStore<MiruTermId, MiruFieldIndexKey> termToIndex = hybridIndex.acquire();
        try {
            return Iterators.transform(termToIndex.iterator(), BulkEntry.<MiruTermId, MiruFieldIndexKey>fromKeyValueStoreEntry());
        } finally {
            hybridIndex.release(termToIndex);
        }
    }
}
