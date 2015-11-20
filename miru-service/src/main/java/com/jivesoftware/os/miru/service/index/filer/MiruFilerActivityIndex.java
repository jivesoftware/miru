package com.jivesoftware.os.miru.service.index.filer;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
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
    public MiruInternalActivity get(final MiruTenantId tenantId, int index, byte[] primitiveBuffer) {
        int capacity = capacity(primitiveBuffer);
        checkArgument(index >= 0 && index < capacity, "Index parameter is out of bounds. The value %s must be >=0 and <%s", index, capacity);
        try {
            return keyedStore.read(FilerIO.intBytes(index), null, (monkey, filer, _primitiveBuffer, lock) -> {
                if (filer != null) {
                    synchronized (lock) {
                        filer.seek(0);
                        return internalActivityMarshaller.fromFiler(tenantId, filer, _primitiveBuffer);
                    }
                } else {
                    return null;
                }
            }, primitiveBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MiruTermId[] get(int index, final int fieldId, byte[] primitiveBuffer) {
        int capacity = capacity(primitiveBuffer);
        checkArgument(index >= 0 && index < capacity, "Index parameter is out of bounds. The value %s must be >=0 and <%s", index, capacity);
        try {
            return keyedStore.read(FilerIO.intBytes(index), null, (monkey, filer, _primitiveBuffer, lock) -> {
                if (filer != null) {
                    synchronized (lock) {
                        filer.seek(0);
                        return internalActivityMarshaller.fieldValueFromFiler(filer, fieldId, _primitiveBuffer);
                    }
                } else {
                    return null;
                }
            }, primitiveBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<MiruTermId[]> getAll(int[] indexes, final int fieldId, byte[] primitiveBuffer) {
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
            return keyedStore.readEach(bytesForIndexes, null, (monkey, filer, _primitiveBuffer, lock) -> {
                if (filer != null) {
                    synchronized (lock) {
                        filer.seek(0);
                        return internalActivityMarshaller.fieldValueFromFiler(filer, fieldId, _primitiveBuffer);
                    }
                } else {
                    return null;
                }
            }, primitiveBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int lastId(byte[] primitiveBuffer) {
        return capacity(primitiveBuffer) - 1;
    }

    @Override
    public void setAndReady(Collection<MiruActivityAndId<MiruInternalActivity>> activityAndIds, byte[] primitiveBuffer) throws Exception {
        if (!activityAndIds.isEmpty()) {
            int lastIndex = setInternal(activityAndIds, primitiveBuffer);
            ready(lastIndex, primitiveBuffer);
        }
    }

    @Override
    public void set(Collection<MiruActivityAndId<MiruInternalActivity>> activityAndIds, byte[] primitiveBuffer) {
        if (!activityAndIds.isEmpty()) {
            setInternal(activityAndIds, primitiveBuffer);
        }
    }

    private int setInternal(Collection<MiruActivityAndId<MiruInternalActivity>> activityAndIds, byte[] primitiveBuffer) {
        int lastIndex = -1;
        for (MiruActivityAndId<MiruInternalActivity> activityAndId : activityAndIds) {
            int index = activityAndId.id;
            lastIndex = Math.max(index, lastIndex);
            MiruInternalActivity activity = activityAndId.activity;
            checkArgument(index >= 0, "Index parameter is out of bounds. The value %s must be >=0", index);
            try {
                final byte[] bytes = internalActivityMarshaller.toBytes(activity, primitiveBuffer);
                keyedStore.writeNewReplace(FilerIO.intBytes(index), (long) 4 + bytes.length, (monkey, newFiler, _primitiveBuffer, newLock) -> {
                    synchronized (newLock) {
                        newFiler.seek(0);
                        FilerIO.write(newFiler, bytes);
                    }
                    return null;
                }, primitiveBuffer);
                log.inc("set>total");
                log.inc("set>bytes", bytes.length);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return lastIndex;
    }

    @Override
    public void ready(int index, byte[] primitiveBuffer) throws Exception {
        log.trace("Check if index {} should extend capacity {}", index, indexSize);
        final int size = index + 1;
        synchronized (indexSize) {
            if (size > indexSize.get()) {
                indexSizeFiler.readWriteAutoGrow(4L, (monkey, filer, _primitiveBuffer, lock) -> {
                    synchronized (lock) {
                        filer.seek(0);
                        FilerIO.writeInt(filer, size, "size", _primitiveBuffer);
                    }
                    return null;
                }, primitiveBuffer);
                log.inc("ready>total");
                log.inc("ready>bytes", 4);
                log.debug("Capacity extended to {}", size);
                indexSize.set(size);
            }
        }
    }

    private int capacity(byte[] primitiveBuffer) {
        try {
            int size = indexSize.get();
            if (size < 0) {
                size = indexSizeFiler.read(null, (monkey, filer, _primitiveBuffer, lock) -> {
                    if (filer != null) {
                        int size1;
                        synchronized (lock) {
                            filer.seek(0);
                            size1 = FilerIO.readInt(filer, "size", _primitiveBuffer);
                        }
                        return size1;
                    } else {
                        return 0;
                    }
                }, primitiveBuffer);
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
