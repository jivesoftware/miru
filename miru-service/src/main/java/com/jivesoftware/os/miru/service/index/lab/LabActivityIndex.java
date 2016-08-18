package com.jivesoftware.os.miru.service.index.lab;

import com.google.common.primitives.Bytes;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.guts.IndexUtil;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.miru.api.activity.TimeAndVersion;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.service.stream.IntTermIdsKeyValueMarshaller;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.mutable.MutableLong;

import static com.google.common.base.Preconditions.checkArgument;

public class LabActivityIndex implements MiruActivityIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider idProvider;
    private final ValueIndex timeAndVersionIndex;
    private final AtomicInteger indexSize = new AtomicInteger(-1);
    private final IntTermIdsKeyValueMarshaller intTermIdsKeyValueMarshaller;
    private final ValueIndex metaIndex;
    private final byte[] metaKey;
    private final ValueIndex[] termStorage;
    private final boolean[] hasTermStorage;

    public LabActivityIndex(OrderIdProvider idProvider,
        ValueIndex timeAndVersionIndex,
        IntTermIdsKeyValueMarshaller intTermIdsKeyValueMarshaller,
        ValueIndex metaIndex,
        byte[] metaKey,
        ValueIndex[] termStorage,
        boolean[] hasTermStorage) {
        this.idProvider = idProvider;
        this.timeAndVersionIndex = timeAndVersionIndex;
        this.intTermIdsKeyValueMarshaller = intTermIdsKeyValueMarshaller;
        this.metaIndex = metaIndex;
        this.metaKey = metaKey;
        this.termStorage = termStorage;
        this.hasTermStorage = hasTermStorage;
    }

    private ValueIndex getTermIndex(int fieldId) {
        return termStorage[fieldId % termStorage.length];
    }

    @Override
    public TimeAndVersion getTimeAndVersion(String name, int index, StackBuffer stackBuffer) throws Exception {
        int capacity = capacity();
        checkArgument(index >= 0 && index < capacity, "Index parameter is out of bounds. The value %s must be >=0 and <%s", index, capacity);

        long[] values = {-1L, -1L};
        timeAndVersionIndex.get((streamKeys) -> streamKeys.key(0, UIO.intBytes(index), 0, 4), (index1, key, timestamp, tombstoned, version, payload) -> {
            if (payload != null && !tombstoned) {
                payload.clear();
                values[0] = payload.getLong(0);
                values[1] = payload.getLong(8);
            }
            return false;
        }, true);

        LOG.inc("count>getTimeAndVersion>total");
        LOG.inc("count>getTimeAndVersion>" + name);
        return (values[0] != -1L || values[1] != -1L) ? new TimeAndVersion(values[0], values[1]) : null;
    }

    @Override
    public TimeAndVersion[] getAllTimeAndVersions(String name, int[] indexes, StackBuffer stackBuffer) throws Exception {
        TimeAndVersion[] tav = new TimeAndVersion[indexes.length];
        timeAndVersionIndex.get(keyStream -> {
            for (int i = 0; i < indexes.length; i++) {
                if (indexes[i] != -1) {
                    byte[] key = UIO.intBytes(indexes[i]);
                    if (!keyStream.key(i, key, 0, metaKey.length)) {
                        return false;
                    }
                }
            }
            return true;
        }, (index1, key, timestamp, tombstoned, version, payload) -> {
            if (payload != null && !tombstoned) {
                payload.clear();
                tav[index1] = new TimeAndVersion(
                    payload.getLong(0),
                    payload.getLong(8));
            }
            return true;
        }, true);

        LOG.inc("count>getTimeAndVersion>total");
        LOG.inc("count>getTimeAndVersion>count", indexes.length);
        LOG.inc("count>getTimeAndVersion>" + name);
        return tav;
    }

    @Override
    public boolean streamTimeAndVersion(StackBuffer stackBuffer, TimeAndVersionStream stream) throws Exception {
        return timeAndVersionIndex.rowScan((index1, key, timestamp, tombstoned, version, payload) -> {
            if (payload != null && !tombstoned) {
                key.clear();
                int id = key.getInt(0);
                payload.clear();
                if (!stream.stream(id, payload.getLong(0), payload.getLong(8))) {
                    return false;
                }
            }
            return true;
        }, true);
    }

    @Override
    public MiruTermId[] get(String name, int index, final int fieldId, StackBuffer stackBuffer) throws Exception {
        if (!hasTermStorage[fieldId] || index > lastId(stackBuffer)) {
            return null;
        }

        MiruTermId[][] termIds = {null};
        byte[] concatKey = Bytes.concat(UIO.intBytes(fieldId), UIO.intBytes(index));
        getTermIndex(fieldId).get((streamKeys) -> streamKeys.key(0, concatKey, 0, concatKey.length),
            (index1, key, timestamp, tombstoned, version, payload) -> {
                if (payload != null && !tombstoned) {
                    payload.clear();
                    byte[] bytes = IndexUtil.toByteArray(payload);
                    termIds[0] = intTermIdsKeyValueMarshaller.bytesValue(null, bytes, 0);
                }
                return false;
            }, true);
        LOG.inc("count>getTerms>total");
        LOG.inc("count>getTerms>" + name);
        return termIds[0];
    }

    @Override
    public MiruTermId[][] getAll(String name, int[] indexes, final int fieldId, StackBuffer stackBuffer) throws Exception {
        return getAll(name, indexes, 0, indexes.length, fieldId, stackBuffer);
    }

    @Override
    public MiruTermId[][] getAll(String name,
        int[] indexes,
        int offset,
        int length,
        final int fieldId,
        StackBuffer stackBuffer) throws Exception {

        if (!hasTermStorage[fieldId]) {
            return null;
        }

        MiruTermId[][] termIds = new MiruTermId[length][];
        ValueIndex termIndex = getTermIndex(fieldId);
        byte[] fieldBytes = UIO.intBytes(fieldId);
        int[] count = {0};
        termIndex.get(
            keyStream -> {
                for (int i = 0; i < length; i++) {
                    int index = indexes[offset + i];
                    if (index >= 0) {
                        byte[] key = Bytes.concat(fieldBytes, UIO.intBytes(index));
                        if (!keyStream.key(i, key, 0, key.length)) {
                            return false;
                        }
                    }
                }
                return true;
            },
            (ki, key, timestamp, tombstoned, version, payload) -> {
                if (payload != null && !tombstoned) {
                    payload.clear();
                    byte[] bytes = IndexUtil.toByteArray(payload);
                    termIds[ki] = intTermIdsKeyValueMarshaller.bytesValue(null, bytes, 0);
                }
                return true;
            }, true);

        LOG.inc("count>getAllTerms>total");
        LOG.inc("count>getAllTerms>count", count[0]);
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
        return capacity() - 1;
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
        StackBuffer stackBuffer) throws Exception {
        if (!activityAndIds.isEmpty()) {
            setInternal(schema, "set", activityAndIds, stackBuffer);
        }
    }

    private int setInternal(MiruSchema schema,
        String name,
        Collection<MiruActivityAndId<MiruInternalActivity>> activityAndIds,
        StackBuffer stackBuffer) throws Exception {

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

        long timestamp = System.currentTimeMillis();
        long version = idProvider.nextId();
        MutableLong bytesWrite = new MutableLong();
        timeAndVersionIndex.append(stream -> {
            for (int j = 0; j < activityAndIdsArray.length; j++) {
                int index = activityAndIdsArray[j].id;
                byte[] payload = new byte[16];
                UIO.longBytes(activityAndIdsArray[j].activity.time, payload, 0);
                UIO.longBytes(activityAndIdsArray[j].activity.version, payload, 8);
                stream.stream(-1, UIO.intBytes(index), timestamp, false, version, payload);
                bytesWrite.add(16);
            }
            return true;
        }, true);

        for (int i = 0; i < schema.fieldCount(); i++) {
            int fieldId = i;
            getTermIndex(fieldId).append(stream -> {
                byte[] fieldBytes = UIO.intBytes(fieldId);
                for (int j = 0; j < activityAndIdsArray.length; j++) {
                    MiruTermId[] termIds = activityAndIdsArray[j].activity.fieldsValues[fieldId];
                    if (termIds != null && termIds.length > 0) {
                        int index = activityAndIdsArray[j].id;
                        byte[] key = Bytes.concat(fieldBytes, UIO.intBytes(index));
                        byte[] payload = intTermIdsKeyValueMarshaller.valueBytes(termIds);
                        stream.stream(-1, key, timestamp, false, version, payload);
                        bytesWrite.add(key.length + payload.length);
                    }
                }
                return true;
            }, true);
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
                long timestamp = System.currentTimeMillis();
                long version = idProvider.nextId();
                metaIndex.append(stream -> stream.stream(-1, metaKey, timestamp, false, version, UIO.intBytes(size)), true);
                LOG.inc("ready>total");
                LOG.inc("ready>bytes", 4);
                LOG.debug("Capacity extended to {}", size);
                indexSize.set(size);
            }
        }
    }

    private int capacity() {
        try {
            int[] size = {indexSize.get()};
            if (size[0] < 0) {
                metaIndex.get((streamKeys) -> streamKeys.key(0, metaKey, 0, metaKey.length),
                    (index, key, timestamp, tombstoned, version, payload) -> {
                        if (payload != null && !tombstoned) {
                            payload.clear();
                            size[0] = payload.getInt(0);
                        }
                        return false;
                    }, true);
                LOG.inc("capacity>total");
                LOG.inc("capacity>bytes", 4);
                indexSize.set(size[0]);
            }
            return size[0];
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
    }
}
