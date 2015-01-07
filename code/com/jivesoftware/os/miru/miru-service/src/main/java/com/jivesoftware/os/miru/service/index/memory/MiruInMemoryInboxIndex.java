package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.map.store.api.KeyValueContext;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueTransaction;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInboxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import java.io.IOException;

/**
 * An in-memory representation of all inbox indexes in a given system
 */
public class MiruInMemoryInboxIndex<BM> implements MiruInboxIndex<BM>,
    BulkImport<Void, KeyedInvertedIndexStream<BM>>,
    BulkExport<Void, KeyedInvertedIndexStream<BM>> {

    private final MiruBitmaps<BM> bitmaps;
    private final KeyValueStore<byte[], ReadWrite<BM>> store;

    public MiruInMemoryInboxIndex(MiruBitmaps<BM> bitmaps,
        KeyValueStore<byte[], ReadWrite<BM>> store) {
        this.bitmaps = bitmaps;
        this.store = store;
    }

    @Override
    public void index(MiruStreamId streamId, int id) throws Exception {
        MiruInvertedIndexAppender inbox = getAppender(streamId);
        inbox.append(id);
    }

    @Override
    public Optional<BM> getInbox(MiruStreamId streamId) throws Exception {
        return new MiruInMemoryInvertedIndex<>(bitmaps, store, streamId.getBytes(), -1).getIndex();
    }

    @Override
    public MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception {
        return new MiruInMemoryInvertedIndex<>(bitmaps, store, streamId.getBytes(), -1);
    }

    @Override
    public int getLastActivityIndex(MiruStreamId streamId) throws Exception {
        return store.execute(streamId.getBytes(), false, getLastActivityIndexTransaction);
    }

    @Override
    public void close() {
    }

    @Override
    public Void bulkExport(MiruTenantId tenantId, final KeyedInvertedIndexStream<BM> callback) throws Exception {
        store.streamKeys(new KeyValueStore.KeyStream<byte[]>() {
            @Override
            public boolean stream(byte[] bytes) throws IOException {
                return callback.stream(bytes, new MiruInMemoryInvertedIndex<>(bitmaps, store, bytes, -1));
            }
        });
        return null;
    }

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<Void, KeyedInvertedIndexStream<BM>> export) throws Exception {
        export.bulkExport(tenantId, new KeyedInvertedIndexStream<BM>() {
            @Override
            public boolean stream(byte[] key, final MiruInvertedIndex<BM> invertedIndex) throws IOException {
                store.execute(key, true, new KeyValueTransaction<ReadWrite<BM>, Void>() {
                    @Override
                    public Void commit(KeyValueContext<ReadWrite<BM>> keyValueContext) throws IOException {
                        try {
                            Optional<BM> index = invertedIndex.getIndex();
                            ReadWrite<BM> readWrite;
                            if (index.isPresent()) {
                                readWrite = new ReadWrite<>(index.get(), invertedIndex.lastId());
                            } else {
                                readWrite = new ReadWrite<>(bitmaps.create(), -1);
                            }
                            keyValueContext.set(readWrite);
                            return null;
                        } catch (Exception e) {
                            throw new IOException("Failed to stream import", e);
                        }
                    }
                });
                return true;
            }
        });
    }

    //TODO pass in
    private final KeyValueTransaction<ReadWrite<BM>, Integer> getLastActivityIndexTransaction = new KeyValueTransaction<ReadWrite<BM>, Integer>() {
        @Override
        public Integer commit(KeyValueContext<ReadWrite<BM>> keyValueContext) throws IOException {
            ReadWrite<BM> readWrite = keyValueContext.get();
            return readWrite != null ? readWrite.lastId : -1;
        }
    };
}
