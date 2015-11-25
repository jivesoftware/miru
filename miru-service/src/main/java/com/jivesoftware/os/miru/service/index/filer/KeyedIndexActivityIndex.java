package com.jivesoftware.os.miru.service.index.filer;

import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.ByteArrayFiler;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
import com.jivesoftware.os.miru.service.index.MiruInternalActivityMarshaller;
import com.jivesoftware.os.miru.service.stream.KeyedIndex;
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
public class KeyedIndexActivityIndex implements MiruActivityIndex {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final KeyedIndex[] keyedIndexes;
    private final AtomicInteger indexSize = new AtomicInteger(-1);
    private final MiruInternalActivityMarshaller internalActivityMarshaller;
    private final int indexSizeIndex;

    public KeyedIndexActivityIndex(KeyedIndex[] keyedIndexes,
        MiruInternalActivityMarshaller internalActivityMarshaller,
        int indexSizeIndex)
        throws Exception {
        this.keyedIndexes = keyedIndexes;
        this.internalActivityMarshaller = internalActivityMarshaller;
        this.indexSizeIndex = indexSizeIndex;
    }

    private KeyedIndex keyedIndex(int index) {
        return keyedIndexes[Math.abs(index % keyedIndexes.length)];
    }

    @Override
    public MiruInternalActivity get(final MiruTenantId tenantId, int index, StackBuffer stackBuffer) {
        int capacity = capacity();
        checkArgument(index >= 0 && index < capacity, "Index parameter is out of bounds. The value %s must be >=0 and <%s", index, capacity);
        try {
            byte[] bytes = keyedIndex(index).get(FilerIO.intBytes(index));
            return bytes == null ? null : internalActivityMarshaller.fromFiler(tenantId, new ByteArrayFiler(bytes), stackBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MiruTermId[] get(int index, final int fieldId, StackBuffer stackBuffer) {
        int capacity = capacity();
        checkArgument(index >= 0 && index < capacity, "Index parameter is out of bounds. The value %s must be >=0 and <%s", index, capacity);
        try {
            byte[] bytes = keyedIndex(index).get(FilerIO.intBytes(index));
            return bytes == null ? null : internalActivityMarshaller.fieldValueFromFiler(new ByteArrayFiler(bytes), fieldId, stackBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<MiruTermId[]> getAll(int[] indexes, final int fieldId, StackBuffer stackBuffer) {
        if (indexes.length == 0) {
            return Collections.emptyList();
        }
        List<MiruTermId[]> result = Lists.newArrayListWithCapacity(indexes.length);
        for (int index : indexes) {
            result.add(get(indexes[index], fieldId, stackBuffer));
        }
        return result;
    }

    @Override
    public int lastId(StackBuffer stackBuffer) {
        return capacity() - 1;
    }

    @Override
    public void setAndReady(Collection<MiruActivityAndId<MiruInternalActivity>> activityAndIds, StackBuffer stackBuffer) throws Exception {
        if (!activityAndIds.isEmpty()) {
            int lastIndex = setInternal(activityAndIds, stackBuffer);
            ready(lastIndex, stackBuffer);
        }
    }

    @Override
    public void set(Collection<MiruActivityAndId<MiruInternalActivity>> activityAndIds, StackBuffer stackBuffer) {
        if (!activityAndIds.isEmpty()) {
            setInternal(activityAndIds, stackBuffer);
        }
    }

    private int setInternal(Collection<MiruActivityAndId<MiruInternalActivity>> activityAndIds, StackBuffer stackBuffer) {
        int lastIndex = -1;
        for (MiruActivityAndId<MiruInternalActivity> activityAndId : activityAndIds) {
            int index = activityAndId.id;
            lastIndex = Math.max(index, lastIndex);
            MiruInternalActivity activity = activityAndId.activity;
            checkArgument(index >= 0, "Index parameter is out of bounds. The value %s must be >=0", index);
            try {
                final byte[] bytes = internalActivityMarshaller.toBytes(activity, stackBuffer);
                keyedIndex(index).put(FilerIO.intBytes(index), bytes);
                log.inc("set>total");
                log.inc("set>bytes", bytes.length);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return lastIndex;
    }

    @Override
    public void ready(int index, StackBuffer stackBuffer) throws Exception {
        log.trace("Check if index {} should extend capacity {}", index, indexSize);
        final int size = index + 1;
        synchronized (indexSize) {
            if (size > indexSize.get()) {
                keyedIndexes[indexSizeIndex].put(FilerIO.intBytes(-1), FilerIO.intBytes(size));
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
                byte[] payload = keyedIndexes[indexSizeIndex].get(FilerIO.intBytes(-1));
                size = FilerIO.bytesInt(payload);
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
