package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.index.MiruField;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;

/**
 * Short-lived (transient) impl. Term dictionary is mem-mapped. Supports index().
 * Next term id is held in memory.
 */
public class MiruHybridField<BM> implements MiruField<BM> {

    private final MiruFieldDefinition fieldDefinition;
    private final MiruInMemoryIndex<BM> index;

    public MiruHybridField(MiruFieldDefinition fieldDefinition,
        MiruInMemoryIndex<BM> index)
        throws Exception {

        this.fieldDefinition = fieldDefinition;
        this.index = index;

        /*
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
        */
    }

    /*
    private interface KeySizeMapChunkFactoryProvider {
        MapChunkFactory getFactory(int keySize);
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
    */

    @Override
    public MiruFieldDefinition getFieldDefinition() {
        return fieldDefinition;
    }

    @Override
    public void notifyStateChange(MiruPartitionState state) throws Exception {
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
    public void index(MiruTermId term, int... ids) throws Exception {
        index.index(fieldDefinition.fieldId, term, ids);
    }

    @Override
    public void remove(MiruTermId term, int id) throws Exception {
        index.remove(fieldDefinition.fieldId, term, id);
    }

    @Override
    public MiruInvertedIndex<BM> getOrCreateInvertedIndex(MiruTermId term) throws Exception {
        Optional<MiruInvertedIndex<BM>> invertedIndex = getInvertedIndex(term);
        if (invertedIndex.isPresent()) {
            return invertedIndex.get();
        }
        return index.allocate(fieldDefinition.fieldId, term);
    }

    @Override
    public Optional<MiruInvertedIndex<BM>> getInvertedIndex(MiruTermId term, int considerIfIndexIdGreaterThanN) throws Exception {
        return index.get(fieldDefinition.fieldId, term, considerIfIndexIdGreaterThanN);
    }

    @Override
    public Optional<MiruInvertedIndex<BM>> getInvertedIndex(MiruTermId term) throws Exception {
        return index.get(fieldDefinition.fieldId, term);
    }

    public MiruInMemoryIndex getIndex() {
        return index;
    }
}
