package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.map.store.VariableKeySizeFileBackMapStore;
import com.jivesoftware.os.jive.utils.map.store.api.KeyValueStoreException;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruField;
import com.jivesoftware.os.miru.service.index.MiruFieldIndexKey;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndex;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

/**
 * Persistent impl. Term dictionary is mem-mapped. Does not support index(), so no next term id.
 */
public class MiruOnDiskField implements MiruField, BulkImport<Map<MiruTermId, MiruFieldIndexKey>> {

    private static final int[] KEY_SIZE_THRESHOLDS = new int[] { 4, 16, 64, 256, 1024 }; //TODO make this configurable per field?
    private static final int PAYLOAD_SIZE = 8; // 2 ints (MiruFieldIndexKey)

    private final int fieldId;
    private final MiruOnDiskIndex index;
    private final VariableKeySizeFileBackMapStore<MiruTermId, MiruFieldIndexKey> termToIndex;

    public MiruOnDiskField(int fieldId, MiruOnDiskIndex index, File mapDirectory) {
        this.fieldId = fieldId;
        this.index = index;

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
    public long sizeInMemory() throws Exception {
        return 0;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return termToIndex.sizeInBytes();
    }

    @Override
    public void index(MiruTermId term, int id) throws Exception {
        throw new UnsupportedOperationException("On disk index is read only");
    }

    @Override
    public void remove(MiruTermId term, int id) throws Exception {
        Optional<MiruInvertedIndex> index = getInvertedIndex(term);
        if (index.isPresent()) {
            index.get().remove(id);
        }
    }

    @Override
    public Optional<MiruInvertedIndex> getInvertedIndex(MiruTermId term, int considerIfIndexIdGreaterThanN) throws Exception {
        Optional<MiruFieldIndexKey> indexKey = getTermId(term);
        if (indexKey.isPresent() && indexKey.get().getMaxId() > considerIfIndexIdGreaterThanN) {
            return getInvertedIndex(term);
        }
        return Optional.absent();
    }

    @Override
    public Optional<MiruInvertedIndex> getInvertedIndex(MiruTermId term) throws Exception {
        Optional<MiruFieldIndexKey> indexKey = getTermId(term);
        if (indexKey.isPresent()) {
            return getInvertedIndex(indexKey.get());
        }
        return Optional.absent();
    }

    public MiruOnDiskIndex getIndex() {
        return index;
    }

    private Optional<MiruInvertedIndex> getInvertedIndex(MiruFieldIndexKey indexKey) throws Exception {
        if (indexKey != null) {
            return index.get(fieldId, indexKey.getId());
        }
        return Optional.absent();
    }

    private Optional<MiruFieldIndexKey> getTermId(MiruTermId term) throws KeyValueStoreException {
        MiruFieldIndexKey indexKey = termToIndex.get(term);
        return Optional.fromNullable(indexKey);
    }

    @Override
    public void bulkImport(BulkExport<Map<MiruTermId, MiruFieldIndexKey>> importItems) throws Exception {
        for (Map.Entry<MiruTermId, MiruFieldIndexKey> entry : importItems.bulkExport().entrySet()) {
            termToIndex.add(entry.getKey(), entry.getValue());
        }
    }
}
