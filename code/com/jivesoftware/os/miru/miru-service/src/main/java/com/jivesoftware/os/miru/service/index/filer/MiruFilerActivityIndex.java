package com.jivesoftware.os.miru.service.index.filer;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.ChunkTransaction;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
import com.jivesoftware.os.miru.service.index.MiruInternalActivityMarshaller;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Chunk-backed impl. Activity data lives in a keyed store, last index is an atomic integer backed by a filer.
 */
public class MiruFilerActivityIndex implements MiruActivityIndex {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final KeyedFilerStore<Long, Void> keyedStore;
    private final AtomicInteger indexSize = new AtomicInteger(-1);
    private final MiruInternalActivityMarshaller internalActivityMarshaller;
    private final MiruFilerProvider<Long, Void> indexSizeFiler;

    public MiruFilerActivityIndex(KeyedFilerStore<Long, Void> keyedStore,
        MiruInternalActivityMarshaller internalActivityMarshaller,
        MiruFilerProvider<Long, Void> indexSizeFiler)
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
            return keyedStore.read(FilerIO.intBytes(index), null, new ChunkTransaction<Void, MiruInternalActivity>() {
                @Override
                public MiruInternalActivity commit(Void monkey, ChunkFiler filer, Object lock) throws IOException {
                    if (filer != null) {
                        synchronized (lock) {
                            filer.seek(0);
                            return internalActivityMarshaller.fromFiler(tenantId, filer);
                        }
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
            return keyedStore.read(FilerIO.intBytes(index), null, new ChunkTransaction<Void, MiruTermId[]>() {
                @Override
                public MiruTermId[] commit(Void monkey, ChunkFiler filer, Object lock) throws IOException {
                    if (filer != null) {
                        synchronized (lock) {
                            filer.seek(0);
                            return internalActivityMarshaller.fieldValueFromFiler(filer, fieldId);
                        }
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
    public List<MiruTermId[]> getAll(MiruTenantId tenantId, int[] indexes, final int fieldId) {
        if (indexes.length == 0) {
            return Collections.emptyList();
        }
        try {
            byte[][] bytesForIndexes = new byte[indexes.length][];
            for (int i = 0; i < indexes.length; i++) {
                if (indexes[i] >= 0) {
                    bytesForIndexes[i] = FilerIO.intBytes(indexes[i]);
                }
            }
            return keyedStore.readEach(bytesForIndexes, null, new ChunkTransaction<Void, MiruTermId[]>() {
                @Override
                public MiruTermId[] commit(Void monkey, ChunkFiler filer, Object lock) throws IOException {
                    if (filer != null) {
                        synchronized (lock) {
                            filer.seek(0);
                            return internalActivityMarshaller.fieldValueFromFiler(filer, fieldId);
                        }
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
    public void setAndReady(Collection<MiruActivityAndId<MiruInternalActivity>> activityAndIds) throws Exception {
        if (!activityAndIds.isEmpty()) {
            int lastIndex = setInternal(activityAndIds);
            ready(lastIndex);
        }
    }

    @Override
    public void set(Collection<MiruActivityAndId<MiruInternalActivity>> activityAndIds) {
        if (!activityAndIds.isEmpty()) {
            setInternal(activityAndIds);
        }
    }

    private int setInternal(Collection<MiruActivityAndId<MiruInternalActivity>> activityAndIds) {
        int lastIndex = -1;
        for (MiruActivityAndId<MiruInternalActivity> activityAndId : activityAndIds) {
            int index = activityAndId.id;
            lastIndex = Math.max(index, lastIndex);
            MiruInternalActivity activity = activityAndId.activity;
            checkArgument(index >= 0, "Index parameter is out of bounds. The value %s must be >=0", index);
            try {
                final byte[] bytes = internalActivityMarshaller.toBytes(activity);
                keyedStore.writeNewReplace(FilerIO.intBytes(index), (long) 4 + bytes.length, new ChunkTransaction<Void, Void>() {
                    @Override
                    public Void commit(Void monkey, ChunkFiler newFiler, Object newLock) throws IOException {
                        synchronized (newLock) {
                            newFiler.seek(0);
                            FilerIO.write(newFiler, bytes);
                        }
                        return null;
                    }
                });
                log.inc("set>total");
                log.inc("set>bytes", bytes.length);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return lastIndex;
    }

    @Override
    public void ready(int index) throws Exception {
        log.trace("Check if index {} should extend capacity {}", index, indexSize);
        final int size = index + 1;
        synchronized (indexSize) {
            if (size > indexSize.get()) {
                indexSizeFiler.readWriteAutoGrow(4L, new ChunkTransaction<Void, Void>() {
                    @Override
                    public Void commit(Void monkey, ChunkFiler filer, Object lock) throws IOException {
                        synchronized (lock) {
                            filer.seek(0);
                            FilerIO.writeInt(filer, size, "size");
                        }
                        return null;
                    }
                });
                log.inc("ready>total");
                log.inc("ready>bytes", 4);
                log.debug("Capacity extended to {}", size);
                indexSize.set(size);
            }
        }
    }

    private int capacity() {
        try {
            int size = indexSize.get();
            if (size < 0) {
                size = indexSizeFiler.read(null, new ChunkTransaction<Void, Integer>() {
                    @Override
                    public Integer commit(Void monkey, ChunkFiler filer, Object lock) throws IOException {
                        if (filer != null) {
                            int size;
                            synchronized (lock) {
                                filer.seek(0);
                                size = FilerIO.readInt(filer, "size");
                            }
                            return size;
                        } else {
                            return 0;
                        }
                    }
                });
                log.inc("capacity>total");
                log.inc("capacity>bytes", 4);
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
}
