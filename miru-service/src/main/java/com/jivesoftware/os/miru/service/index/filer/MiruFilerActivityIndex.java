package com.jivesoftware.os.miru.service.index.filer;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.HintAndTransaction;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.api.StackBuffer;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.mutable.MutableLong;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Chunk-backed impl. Activity data lives in a keyed store, last index is an atomic integer backed by a filer.
 */
public class MiruFilerActivityIndex implements MiruActivityIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

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
    public MiruInternalActivity get(String name, final MiruTenantId tenantId, int index, StackBuffer stackBuffer) throws IOException, InterruptedException {
        int capacity = capacity(stackBuffer);
        checkArgument(index >= 0 && index < capacity, "Index parameter is out of bounds. The value %s must be >=0 and <%s", index, capacity);
        MutableLong bytes = new MutableLong();
        MiruInternalActivity activity = keyedStore.read(FilerIO.intBytes(index), null, (monkey, filer, _stackBuffer, lock) -> {
            if (filer != null) {
                bytes.add(filer.length());
                synchronized (lock) {
                    filer.seek(0);
                    return internalActivityMarshaller.fromFiler(tenantId, filer, _stackBuffer);
                }
            } else {
                return null;
            }
        }, stackBuffer);
        LOG.inc("count>getActivity>total");
        LOG.inc("count>getActivity>" + name);
        LOG.inc("bytes>getActivity>total", bytes.longValue());
        LOG.inc("bytes>getActivity>" + name, bytes.longValue());
        return activity;
    }

    @Override
    public MiruTermId[] get(String name, int index, final int fieldId, StackBuffer stackBuffer) throws IOException, InterruptedException {
        int capacity = capacity(stackBuffer);
        checkArgument(index >= 0 && index < capacity, "Index parameter is out of bounds. The value %s must be >=0 and <%s", index, capacity);
        MiruTermId[] termIds = keyedStore.read(FilerIO.intBytes(index), null, (monkey, filer, _stackBuffer, lock) -> {
            if (filer != null) {
                synchronized (lock) {
                    filer.seek(0);
                    return internalActivityMarshaller.fieldValueFromFiler(filer, fieldId, _stackBuffer);
                }
            } else {
                return null;
            }
        }, stackBuffer);
        long bytes = 0;
        for (MiruTermId termId : termIds) {
            bytes += termId.length();
        }
        LOG.inc("count>getTerms>total");
        LOG.inc("count>getTerms>" + name);
        LOG.inc("bytes>getTerms>total", bytes);
        LOG.inc("bytes>getTerms>" + name, bytes);
        return termIds;
    }

    @Override
    public List<MiruTermId[]> getAll(String name, int[] indexes, final int fieldId, StackBuffer stackBuffer) throws IOException, InterruptedException {
        if (indexes.length == 0) {
            return Collections.emptyList();
        }
        byte[][] bytesForIndexes = new byte[indexes.length][];
        for (int i = 0; i < indexes.length; i++) {
            if (indexes[i] >= 0) {
                bytesForIndexes[i] = FilerIO.intBytes(indexes[i]);
            }
        }

        MiruTermId[][] results = new MiruTermId[indexes.length][];
        keyedStore.readEach(bytesForIndexes, null, (monkey, filer, _stackBuffer, lock, index) -> {
            if (filer != null) {
                synchronized (lock) {
                    filer.seek(0);
                    return internalActivityMarshaller.fieldValueFromFiler(filer, fieldId, _stackBuffer);
                }
            } else {
                return null;
            }
        }, results, stackBuffer);

        long bytes = 0;
        for (MiruTermId[] termIds : results) {
            if (termIds != null) {
                for (MiruTermId termId : termIds) {
                    if (termId != null) {
                        bytes += termId.length();
                    }
                }
            }
        }
        LOG.inc("count>getAllTerms>total");
        LOG.inc("count>getAllTerms>" + name);
        LOG.inc("bytes>getAllTerms>total", bytes);
        LOG.inc("bytes>getAllTerms>" + name, bytes);
        return Arrays.asList(results);

    }

    @Override
    public int lastId(StackBuffer stackBuffer) {
        return capacity(stackBuffer) - 1;
    }

    @Override
    public void setAndReady(Collection<MiruActivityAndId<MiruInternalActivity>> activityAndIds, StackBuffer stackBuffer) throws Exception {
        if (!activityAndIds.isEmpty()) {
            int lastIndex = setInternal("setAndReady", activityAndIds, stackBuffer);
            ready(lastIndex, stackBuffer);
        }
    }

    @Override
    public void set(Collection<MiruActivityAndId<MiruInternalActivity>> activityAndIds, StackBuffer stackBuffer) throws IOException, InterruptedException {
        if (!activityAndIds.isEmpty()) {
            setInternal("set", activityAndIds, stackBuffer);
        }
    }

    private int setInternal(String name, Collection<MiruActivityAndId<MiruInternalActivity>> activityAndIds,
        StackBuffer stackBuffer) throws IOException, InterruptedException {

        int lastIndex = -1;
        @SuppressWarnings("unchecked")
        MiruActivityAndId<MiruInternalActivity>[] activityAndIdsArray = activityAndIds.toArray(new MiruActivityAndId[activityAndIds.size()]);
        byte[][] keyBytes = new byte[activityAndIdsArray.length][];
        for (int i = 0; i < activityAndIdsArray.length; i++) {
            int index = activityAndIdsArray[i].id;
            checkArgument(index >= 0, "Index parameter is out of bounds. The value %s must be >=0", index);
            lastIndex = Math.max(index, lastIndex);
            keyBytes[i] = FilerIO.intBytes(activityAndIdsArray[i].id);
        }
        MutableLong bytesWrite = new MutableLong();
        keyedStore.multiWriteNewReplace(keyBytes,
            (oldMonkey, oldFiler, stackBuffer1, oldLock, index) -> {
                final byte[] bytes = internalActivityMarshaller.toBytes(activityAndIdsArray[index].activity, stackBuffer1);
                long filerSize = (long) 4 + bytes.length;
                bytesWrite.add(filerSize);
                LOG.inc("set>total");
                LOG.inc("set>bytes", bytes.length);
                return new HintAndTransaction<>(filerSize, (newMonkey, newFiler, stackBuffer2, newLock) -> {
                    synchronized (newLock) {
                        newFiler.seek(0);
                        FilerIO.write(newFiler, bytes);
                    }
                    return null;
                });
            },
            new Void[activityAndIdsArray.length],
            stackBuffer);
        LOG.inc("count>get>total");
        LOG.inc("count>get>" + name);
        LOG.inc("bytes>get>total", bytesWrite.longValue());
        LOG.inc("bytes>get>" + name, bytesWrite.longValue());

        return lastIndex;
    }

    @Override
    public void ready(int index, StackBuffer stackBuffer) throws Exception {
        LOG.trace("Check if index {} should extend capacity {}", index, indexSize);
        final int size = index + 1;
        synchronized (indexSize) {
            if (size > indexSize.get()) {
                indexSizeFiler.readWriteAutoGrow(4L, (monkey, filer, _stackBuffer, lock) -> {
                    synchronized (lock) {
                        filer.seek(0);
                        FilerIO.writeInt(filer, size, "size", _stackBuffer);
                    }
                    return null;
                }, stackBuffer);
                LOG.inc("ready>total");
                LOG.inc("ready>bytes", 4);
                LOG.debug("Capacity extended to {}", size);
                indexSize.set(size);
            }
        }
    }

    private int capacity(StackBuffer stackBuffer) {
        try {
            int size = indexSize.get();
            if (size < 0) {
                size = indexSizeFiler.read(null, (monkey, filer, _stackBuffer, lock) -> {
                    if (filer != null) {
                        int size1;
                        synchronized (lock) {
                            filer.seek(0);
                            size1 = FilerIO.readInt(filer, "size", _stackBuffer);
                        }
                        return size1;
                    } else {
                        return 0;
                    }
                }, stackBuffer);
                LOG.inc("capacity>total");
                LOG.inc("capacity>bytes", 4);
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
