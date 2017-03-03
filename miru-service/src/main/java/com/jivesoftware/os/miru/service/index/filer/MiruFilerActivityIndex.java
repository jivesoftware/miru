package com.jivesoftware.os.miru.service.index.filer;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.KeyValueContext;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.keyed.store.TxKeyValueStore;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.plugin.index.TimeVersionRealtime;
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
import com.jivesoftware.os.miru.service.stream.IntTermIdsKeyValueMarshaller;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.mutable.MutableLong;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Chunk-backed impl. Activity data lives in a keyed store, last index is an atomic integer backed by a filer.
 */
public class MiruFilerActivityIndex implements MiruActivityIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruFilerProvider<Long, Void> timeAndVersionFiler;
    private final AtomicInteger indexSize = new AtomicInteger(-1);
    private final IntTermIdsKeyValueMarshaller intTermIdsKeyValueMarshaller;
    private final MiruFilerProvider<Long, Void> indexSizeFiler;
    private final MiruFilerProvider<Long, Void>[] termLookup;
    private final TxKeyValueStore<Integer, MiruTermId[]>[][] termStorage;

    public MiruFilerActivityIndex(MiruFilerProvider<Long, Void> timeAndVersionFiler,
        IntTermIdsKeyValueMarshaller intTermIdsKeyValueMarshaller,
        MiruFilerProvider<Long, Void> indexSizeFiler,
        MiruFilerProvider<Long, Void>[] termLookup,
        TxKeyValueStore<Integer, MiruTermId[]>[][] termStorage)
        throws Exception {
        this.timeAndVersionFiler = timeAndVersionFiler;
        this.intTermIdsKeyValueMarshaller = intTermIdsKeyValueMarshaller;
        this.indexSizeFiler = indexSizeFiler;
        this.termLookup = termLookup;
        this.termStorage = termStorage;
    }

    @Override
    public TimeVersionRealtime getTimeVersionRealtime(String name, int index, StackBuffer stackBuffer) throws IOException, InterruptedException {
        int capacity = capacity(stackBuffer);
        checkArgument(index >= 0 && index < capacity, "Index parameter is out of bounds. The value %s must be >=0 and <%s", index, capacity);

        long[] values = { -1L, -1L };
        timeAndVersionFiler.read(null,
            (monkey, filer, stackBuffer1, lock) -> {
                if (filer != null) {
                    synchronized (lock) {
                        int offset = index * 16;
                        if (filer.length() >= offset + 16) {
                            filer.seek(offset);
                            values[0] = FilerIO.readLong(filer, "time", stackBuffer1);
                            values[1] = FilerIO.readLong(filer, "version", stackBuffer1);
                        }
                    }
                }
                return null;
            },
            stackBuffer);

        LOG.inc("count>getTimeVersionRealtime>total");
        LOG.inc("count>getTimeVersionRealtime>" + name);
        return (values[0] != -1L || values[1] != -1L) ? new TimeVersionRealtime(values[0], values[1], -1L, false) : null;
    }

    @Override
    public TimeVersionRealtime[] getAllTimeVersionRealtime(String name, int[] indexes, StackBuffer stackBuffer) throws Exception {
        TimeVersionRealtime[] tav = new TimeVersionRealtime[indexes.length];
        timeAndVersionFiler.read(null,
            (monkey, filer, stackBuffer1, lock) -> {
                if (filer != null) {
                    synchronized (lock) {
                        for (int i = 0; i < indexes.length; i++) {
                            int index = indexes[i];
                            if (index >= 0) {
                                int offset = index * 16;
                                if (filer.length() >= offset + 16) {
                                    filer.seek(offset);
                                    tav[i] = new TimeVersionRealtime(
                                        FilerIO.readLong(filer, "time", stackBuffer1),
                                        FilerIO.readLong(filer, "version", stackBuffer1),
                                        -1L,
                                        false);
                                }
                            }
                        }
                    }
                }
                return null;
            },
            stackBuffer);

        LOG.inc("count>getAllTimeVersionRealtime>total");
        LOG.inc("count>getAllTimeVersionRealtime>count", indexes.length);
        LOG.inc("count>getAllTimeVersionRealtime>" + name);
        return tav;
    }

    @Override
    public boolean streamTimeVersionRealtime(StackBuffer stackBuffer, TimeVersionRealtimeStream stream) throws Exception {
        throw new UnsupportedOperationException("Hopefully never");
    }

    @Override
    public MiruTermId[] get(String name,
        int index,
        final MiruFieldDefinition fieldDefinition,
        StackBuffer stackBuffer) throws IOException, InterruptedException {
        int fieldId = fieldDefinition.fieldId;
        if (termLookup[fieldId] == null || index > lastId(stackBuffer)) {
            return null;
        }

        int[] valuePower = { -1 };
        termLookup[fieldId].read(null,
            (monkey, filer, stackBuffer1, lock) -> {
                if (filer != null) {
                    synchronized (lock) {
                        if (filer.length() >= index + 1) {
                            filer.seek(index);
                            valuePower[0] = readUnsignedByte(filer);
                        }
                    }
                }
                return null;
            },
            stackBuffer);

        MiruTermId[] termIds = valuePower[0] == -1 ? null :
            termStorage[(valuePower[0] & 0xFF)][fieldId].execute(index, false, KeyValueContext::get, stackBuffer);

        LOG.inc("count>getTerms>total");
        LOG.inc("count>getTerms>" + name);
        return termIds;
    }

    private int readUnsignedByte(ChunkFiler filer) throws IOException {
        return (filer.read() & 0xFF);
    }

    private void writeUnsignedByte(ChunkFiler filer, int value) throws IOException {
        filer.write(value & 0xFF);
    }

    @Override
    public MiruTermId[][] getAll(String name,
        int[] indexes,
        final MiruFieldDefinition fieldDefinition,
        StackBuffer stackBuffer) throws IOException, InterruptedException {
        return getAll(name, indexes, 0, indexes.length, fieldDefinition, stackBuffer);
    }

    @Override
    public MiruTermId[][] getAll(String name,
        int[] indexes,
        int offset,
        int length,
        final MiruFieldDefinition fieldDefinition,
        StackBuffer stackBuffer) throws IOException, InterruptedException {

        int fieldId = fieldDefinition.fieldId;
        if (termLookup[fieldId] == null) {
            return null;
        }

        Integer[] keys = new Integer[length];
        for (int i = 0; i < length; i++) {
            if (indexes[offset + i] >= 0) {
                keys[i] = indexes[offset + i];
            }
        }

        boolean[][] valuePowers = new boolean[16][];
        termLookup[fieldId].read(null,
            (monkey, filer, stackBuffer1, lock) -> {
                if (filer != null) {
                    synchronized (lock) {
                        long filerLength = filer.length();
                        for (int i = 0; i < length; i++) {
                            int index = indexes[offset + i];
                            if (index == -1) {
                                continue;
                            }
                            if (filerLength >= index + 1) {
                                filer.seek(index);
                                int valuePower = readUnsignedByte(filer);
                                if (valuePower > 0) {
                                    if (valuePowers[valuePower] == null) {
                                        valuePowers[valuePower] = new boolean[length];
                                    }
                                    valuePowers[valuePower][i] = true;
                                }
                            }
                        }
                    }
                }
                return null;
            },
            stackBuffer);

        MiruTermId[][] termIds = new MiruTermId[length][];
        for (int valuePower = 0; valuePower < valuePowers.length; valuePower++) {
            if (valuePowers[valuePower] != null) {
                Integer[] valueKeys = new Integer[length];
                for (int j = 0; j < length; j++) {
                    if (valuePowers[valuePower][j]) {
                        valueKeys[j] = keys[j];
                    }
                }
                termStorage[valuePower][fieldId].multiExecute(valueKeys,
                    (keyValueContext, index) -> termIds[index] = keyValueContext.get(),
                    stackBuffer);
            }
        }
        LOG.inc("count>getAllTerms>total");
        LOG.inc("count>getAllTerms>" + name);
        return termIds;
    }

    @Override
    public MiruIBA[] getProp(String name, int index, int propId, StackBuffer stackBuffer) {
        throw new UnsupportedOperationException("not yet");
    }

    @Override
    public String[] getAuthz(String name, int index, StackBuffer stackBuffer) {
        return null;
    }

    @Override
    public int lastId(StackBuffer stackBuffer) {
        return capacity(stackBuffer) - 1;
    }

    @Override
    public void setAndReady(MiruSchema schema, Collection<MiruActivityAndId<MiruInternalActivity>> activityAndIds, StackBuffer stackBuffer) throws Exception {
        if (!activityAndIds.isEmpty()) {
            int lastIndex = setInternal(schema, "setAndReady", activityAndIds, stackBuffer);
            ready(lastIndex, stackBuffer);
        }
    }

    @Override
    public void set(MiruSchema schema,
        Collection<MiruActivityAndId<MiruInternalActivity>> activityAndIds,
        StackBuffer stackBuffer) throws IOException, InterruptedException {
        if (!activityAndIds.isEmpty()) {
            setInternal(schema, "set", activityAndIds, stackBuffer);
        }
    }

    private int setInternal(MiruSchema schema,
        String name,
        Collection<MiruActivityAndId<MiruInternalActivity>> activityAndIds,
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

        int capacity = lastIndex + 1;

        MutableLong bytesWrite = new MutableLong();
        timeAndVersionFiler.readWriteAutoGrow(capacity * 16L,
            (monkey, filer, stackBuffer1, lock) -> {
                synchronized (lock) {
                    LOG.inc("set>total");
                    LOG.inc("set>bytes", 16);
                    for (int j = 0; j < activityAndIdsArray.length; j++) {
                        int index = activityAndIdsArray[j].id;
                        filer.seek(index * 16);
                        FilerIO.writeLong(filer, activityAndIdsArray[j].activity.time, "time", stackBuffer1);
                        FilerIO.writeLong(filer, activityAndIdsArray[j].activity.version, "version", stackBuffer1);
                        bytesWrite.add(16);
                    }
                }
                return null;
            },
            stackBuffer);

        for (int i = 0; i < termLookup.length; i++) {
            int fieldId = i;
            LOG.inc("count>set>lookupFields");
            if (termLookup[fieldId] != null) {
                @SuppressWarnings("unchecked")
                boolean[][] valuePowers = new boolean[16][];
                termLookup[fieldId].readWriteAutoGrow((long) capacity,
                    (monkey, filer, stackBuffer1, lock) -> {
                        synchronized (lock) {
                            LOG.inc("count>set>termLookup");
                            for (int j = 0; j < activityAndIdsArray.length; j++) {
                                MiruTermId[] termIds = activityAndIdsArray[j].activity.fieldsValues[fieldId];
                                if (termIds != null && termIds.length > 0) {
                                    int index = activityAndIdsArray[j].id;
                                    int valueSize = intTermIdsKeyValueMarshaller.valueSizeInBytes(termIds);
                                    int valuePower = FilerIO.chunkPower(valueSize, 1);
                                    if (valuePowers[valuePower] == null) {
                                        valuePowers[valuePower] = new boolean[activityAndIdsArray.length];
                                    }
                                    valuePowers[valuePower][j] = true;

                                    filer.seek(index);
                                    writeUnsignedByte(filer, valuePower);
                                    bytesWrite.add(1 + 4 + valueSize); // power byte plus key/value payload below
                                }
                            }
                        }
                        return null;
                    },
                    stackBuffer);

                for (int valuePower = 0; valuePower < valuePowers.length; valuePower++) {
                    if (valuePowers[valuePower] != null && termStorage[valuePower][fieldId] != null) {
                        Integer[] valueKeys = new Integer[activityAndIdsArray.length];
                        for (int j = 0; j < valueKeys.length; j++) {
                            if (valuePowers[valuePower][j]) {
                                valueKeys[j] = activityAndIdsArray[j].id;
                            }
                        }
                        termStorage[valuePower][fieldId].multiExecute(valueKeys,
                            (keyValueContext, index) -> {
                                LOG.inc("count>set>termStorage");
                                // bytes written metric handled above
                                keyValueContext.set(activityAndIdsArray[index].activity.fieldsValues[fieldId]);
                            },
                            stackBuffer);
                    }
                }
            }
        }

        LOG.inc("count>set>total");
        LOG.inc("count>set>" + name);
        LOG.inc("bytes>set>total", bytesWrite.longValue());
        LOG.inc("bytes>set>" + name, bytesWrite.longValue());

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
