package com.jivesoftware.os.miru.service.index.disk;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.filer.keyed.store.KeyedFilerStore;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.BulkStream;
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
import com.jivesoftware.os.miru.service.index.MiruInternalActivityMarshaller;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Chunk-backed impl. Activity data lives in a keyed store, last index is an atomic integer backed by a filer.
 */
public class MiruChunkActivityIndex implements MiruActivityIndex,
    BulkImport<Void, BulkStream<MiruInternalActivity>>,
    BulkExport<Void, BulkStream<MiruInternalActivity>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final KeyedFilerStore keyedStore;
    private final AtomicInteger indexSize = new AtomicInteger(-1);
    private final MiruInternalActivityMarshaller internalActivityMarshaller;
    private final MiruFilerProvider indexSizeFiler;

    public MiruChunkActivityIndex(KeyedFilerStore keyedStore,
        MiruInternalActivityMarshaller internalActivityMarshaller,
        MiruFilerProvider indexSizeFiler)
        throws Exception {
        this.keyedStore = keyedStore;
        this.internalActivityMarshaller = internalActivityMarshaller;
        this.indexSizeFiler = indexSizeFiler;
    }

    @Override
    public MiruInternalActivity get(final MiruTenantId tenantId, int index) {
        int capacity = capacity();
        checkArgument(index >= 0 && index < capacity, "Index parameter is out of bounds. The value %s must be >=0 and <%s", index, capacity);
        try {
            return keyedStore.execute(FilerIO.intBytes(index), -1, new FilerTransaction<Filer, MiruInternalActivity>() {
                @Override
                public MiruInternalActivity commit(Filer filer) throws IOException {
                    if (filer != null) {
                        filer.seek(0);
                        return internalActivityMarshaller.fromFiler(tenantId, filer);
                    } else {
                        return null;
                    }
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MiruTermId[] get(MiruTenantId tenantId, int index, final int fieldId) {
        int capacity = capacity();
        checkArgument(index >= 0 && index < capacity, "Index parameter is out of bounds. The value %s must be >=0 and <%s", index, capacity);
        try {
            return keyedStore.execute(FilerIO.intBytes(index), -1, new FilerTransaction<Filer, MiruTermId[]>() {
                @Override
                public MiruTermId[] commit(Filer filer) throws IOException {
                    if (filer != null) {
                        filer.seek(0);
                        return internalActivityMarshaller.fieldValueFromFiler(filer, fieldId);
                    } else {
                        return null;
                    }
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int lastId() {
        return capacity() - 1;
    }

    @Override
    public void setAndReady(List<MiruActivityAndId<MiruInternalActivity>> activityAndIds) throws Exception {
        if (!activityAndIds.isEmpty()) {
            set(activityAndIds);
            ready(activityAndIds.get(activityAndIds.size() - 1).id);
        }
    }

    @Override
    public void set(List<MiruActivityAndId<MiruInternalActivity>> activityAndIds) {
        for (MiruActivityAndId<MiruInternalActivity> activityAndId : activityAndIds) {
            int index = activityAndId.id;
            MiruInternalActivity activity = activityAndId.activity;
            checkArgument(index >= 0, "Index parameter is out of bounds. The value %s must be >=0", index);
            try {
                //byte[] bytes = objectMapper.writeValueAsBytes(activity);
                final byte[] bytes = internalActivityMarshaller.toBytes(activity);
                keyedStore.execute(FilerIO.intBytes(index), 4 + bytes.length, new FilerTransaction<Filer, Void>() {
                    @Override
                    public Void commit(Filer filer) throws IOException {
                        filer.seek(0);
                        FilerIO.write(filer, bytes);
                        return null;
                    }
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void ready(int index) throws Exception {
        log.trace("Check if index {} should extend capacity {}", index, indexSize);
        final int size = index + 1;
        synchronized (indexSize) {
            if (size > indexSize.get()) {
                indexSizeFiler.execute(4, new FilerTransaction<Filer, Void>() {
                    @Override
                    public Void commit(Filer chunkFiler) throws IOException {
                        chunkFiler.seek(0);
                        FilerIO.writeInt(chunkFiler, size, "size");
                        return null;
                    }
                });
                log.debug("Capacity extended to {}", size);
                indexSize.set(size);
            }
        }
    }

    private int capacity() {
        try {
            int size = indexSize.get();
            if (size < 0) {
                size = indexSizeFiler.execute(-1, new FilerTransaction<Filer, Integer>() {
                    @Override
                    public Integer commit(Filer chunkFiler) throws IOException {
                        if (chunkFiler != null) {
                            chunkFiler.seek(0);
                            return FilerIO.readInt(chunkFiler, "size");
                        } else {
                            return 0;
                        }
                    }
                });
                indexSize.set(size);
            }
            return size;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<Void, BulkStream<MiruInternalActivity>> export) throws Exception {
        final int batchSize = 1_000; //TODO expose to config
        final List<MiruActivityAndId<MiruInternalActivity>> batch = new ArrayList<>(batchSize);
        final AtomicInteger index = new AtomicInteger(0);
        export.bulkExport(tenantId, new BulkStream<MiruInternalActivity>() {
            @Override
            public boolean stream(MiruInternalActivity activity) throws Exception {
                if (activity == null) {
                    return false;
                }
                batch.add(new MiruActivityAndId<>(activity, index.get()));
                if (batch.size() >= batchSize) {
                    set(batch);
                    batch.clear();
                }
                index.incrementAndGet();
                return true;
            }
        });

        if (!batch.isEmpty()) {
            setAndReady(batch);
        }

        ready(index.get() - 1);
    }

    @Override
    public Void bulkExport(MiruTenantId tenantId, BulkStream<MiruInternalActivity> callback) throws Exception {
        int capacity = capacity();
        for (int index = 0; index < capacity; index++) {
            if (!callback.stream(get(tenantId, index))) {
                break;
            }
        }
        return null;
    }
}
