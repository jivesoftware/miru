package com.jivesoftware.os.miru.service.index.memory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.chunk.store.MultiChunkStore;
import com.jivesoftware.os.jive.utils.io.ByteArrayFiler;
import com.jivesoftware.os.jive.utils.io.Filer;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.jive.utils.keyed.store.FileBackedKeyedStore;
import com.jivesoftware.os.jive.utils.keyed.store.SwappableFiler;
import com.jivesoftware.os.jive.utils.keyed.store.SwappingFiler;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.service.activity.MiruInternalActivity;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruActivityIndex;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Short-lived (transient) impl. Like the mem-mapped impl, activity data is mem-mapped. However, set() is supported. The last index is only held in memory (not
 * stored on disk).
 */
public class MiruHybridActivityIndex implements MiruActivityIndex, BulkImport<MiruInternalActivity[]>, BulkExport<MiruInternalActivity[]> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final FileBackedKeyedStore keyedStore;
    private final AtomicInteger indexSize = new AtomicInteger();

    public MiruHybridActivityIndex(File mapDirectory, File swapDirectory, MultiChunkStore chunkStore, ObjectMapper objectMapper) throws Exception {
        this.keyedStore = new FileBackedKeyedStore(mapDirectory.getAbsolutePath(), swapDirectory.getAbsolutePath(), 4, 100, chunkStore, 512);
    }

    @Override
    public MiruInternalActivity get(int index) {
        int capacity = indexSize.get();
        checkArgument(index >= 0 && index < capacity, "Index parameter is out of bounds. The value " + index + " must be >=0 and <" + capacity);
        try {
            SwappableFiler swappableFiler = keyedStore.get(FilerIO.intBytes(index), false);
            if (swappableFiler != null) {
                synchronized (swappableFiler.lock()) {
                    swappableFiler.sync();
                    swappableFiler.seek(0);
                    return fromFiler(swappableFiler);
                }
            }
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MiruTermId[] get(int index, int fieldId) {
        int capacity = indexSize.get();
        checkArgument(index >= 0 && index < capacity, "Index parameter is out of bounds. The value " + index + " must be >=0 and <" + capacity);
        try {
            SwappableFiler swappableFiler = keyedStore.get(FilerIO.intBytes(index), false);
            if (swappableFiler != null) {
                synchronized (swappableFiler.lock()) {
                    swappableFiler.sync();
                    swappableFiler.seek(0);
                    swappableFiler.seek(fieldId * 4);
                    int offset = FilerIO.readInt(swappableFiler, "");
                    swappableFiler.seek(offset);
                    int valueCount = FilerIO.readInt(swappableFiler, "");
                    if (valueCount >= 0) {
                        MiruTermId[] termIds = new MiruTermId[valueCount];
                        for (int i = 0; i < valueCount; i++) {
                            int length = FilerIO.readInt(swappableFiler, "");
                            byte[] value = new byte[length];
                            FilerIO.read(swappableFiler, value);
                            termIds[i] = new MiruTermId(value);
                        }
                        return termIds;
                    } else {
                        return null;
                    }
                }
            }
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private MiruInternalActivity fromFiler(Filer filer) throws IOException {
        int fieldsLength = FilerIO.readInt(filer, "fieldsLength");
        int propsLength = FilerIO.readInt(filer, "propsLength");
        MiruTermId[][] values = new MiruTermId[fieldsLength][];
        MiruIBA[][] props = new MiruIBA[propsLength][];
        for (int i = 0; i < fieldsLength + propsLength; i++) {
            FilerIO.readInt(filer, "offest");
        }
        for (int i = 0; i < fieldsLength; i++) {
            values[i] = valuesFromFilter(filer);
        }
        for (int i = 0; i < propsLength; i++) {
            props[i] = propsFromFilter(filer);
        }

        long time = FilerIO.readLong(filer, "time");
        long version = FilerIO.readLong(filer, "version");
        byte[] tenantId = FilerIO.readByteArray(filer, "tenantId");
        String[] authz = FilerIO.readStringArray(filer, "authz");

        return new MiruInternalActivity(new MiruTenantId(tenantId), time, authz, version, values, props);
    }

    private MiruTermId[] valuesFromFilter(Filer filer) throws IOException {
        int length = FilerIO.readInt(filer, "length");
        if (length == -1) {
            return null;
        }
        MiruTermId[] terms = new MiruTermId[length];
        for (int i = 0; i < length; i++) {
            int l = FilerIO.readInt(filer, "length");
            byte[] bytes = new byte[l];
            FilerIO.read(filer, bytes);
            terms[i] = new MiruTermId(bytes);
        }

        return terms;
    }

    private MiruIBA[] propsFromFilter(Filer filer) throws IOException {
        int length = FilerIO.readInt(filer, "length");
        if (length == -1) {
            return null;
        }
        MiruIBA[] terms = new MiruIBA[length];
        for (int i = 0; i < length; i++) {
            int l = FilerIO.readInt(filer, "length");
            byte[] bytes = new byte[l];
            FilerIO.read(filer, bytes);
            terms[i] = new MiruIBA(bytes);
        }

        return terms;
    }

    private byte[] toBytes(MiruInternalActivity activity) throws IOException {

        int fieldsLength = activity.fieldsValues.length;
        int propsLength = activity.propsValues.length;

        byte[][] valueBytes = new byte[fieldsLength + propsLength][];
        int[] offsetIndex = new int[fieldsLength + propsLength];
        // fieldsLength + propsLength + fieldsIndex * 4 + propsIndex * 4
        int offset = (4 + 4) + (fieldsLength * 4) + (propsLength * 4);
        for (int i = 0; i < fieldsLength; i++) {
            offsetIndex[i] = offset;
            valueBytes[i] = fieldValuesToBytes(activity.fieldsValues[i]);
            offset += valueBytes[i].length;
        }

        for (int i = fieldsLength; i < fieldsLength + propsLength; i++) {
            offsetIndex[i] = offset;
            valueBytes[i] = fieldValuesToBytes(activity.propsValues[i]);
            offset += valueBytes[i].length;
        }

        ByteArrayFiler filer = new ByteArrayFiler();
        FilerIO.writeInt(filer, fieldsLength, "fieldsLength");
        FilerIO.writeInt(filer, propsLength, "propsLength");
        for (int index : offsetIndex) {
            FilerIO.writeInt(filer, index, "index");
        }
        for (int i = 0; i < fieldsLength; i++) {
            FilerIO.write(filer, valueBytes[i]);
        }

        FilerIO.writeLong(filer, activity.time, "time");
        FilerIO.writeLong(filer, activity.version, "version");
        FilerIO.writeByteArray(filer, activity.tenantId.immutableBytes(), "tenantId");
        FilerIO.writeStringArray(filer, activity.authz, "authz");
        return filer.getBytes();
    }

    private byte[] fieldValuesToBytes(MiruIBA[] values) throws IOException {
        ByteArrayFiler filer = new ByteArrayFiler();
        if (values == null) {
            FilerIO.writeInt(filer, -1, "length");
        } else {
            FilerIO.writeInt(filer, values.length, "length");
            for (MiruIBA v : values) {
                byte[] bytes = v.immutableBytes();
                FilerIO.writeInt(filer, bytes.length, "length");
                FilerIO.write(filer, bytes);
            }
        }
        return filer.getBytes();
    }

    @Override
    public int lastId() {
        return indexSize.get() - 1;
    }

    @Override
    public void set(int index, MiruInternalActivity activity) {
        checkArgument(index >= 0, "Index parameter is out of bounds. The value " + index + " must be >=0");
        try {
            //byte[] bytes = objectMapper.writeValueAsBytes(activity);
            byte[] bytes = toBytes(activity);
            SwappableFiler swappableFiler = keyedStore.get(FilerIO.intBytes(index), true);
            synchronized (swappableFiler.lock()) {
                SwappingFiler swappingFiler = swappableFiler.swap(4 + bytes.length);
                //FilerIO.writeByteArray(swappingFiler, bytes, "activity");
                FilerIO.write(swappingFiler, bytes);
                swappingFiler.commit();
            }
            checkCapacity(index);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long sizeInMemory() {
        return 0;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return keyedStore.mapStoreSizeInBytes();
    }

    private void checkCapacity(int index) {
        log.trace("Check if index {} should extend capacity {}", index, indexSize);
        int size = index + 1;
        synchronized (indexSize) {
            if (size > indexSize.get()) {
                log.debug("Capacity extended to {}", size);
                indexSize.set(size);
            }
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void bulkImport(BulkExport<MiruInternalActivity[]> bulkExport) throws Exception {
        MiruInternalActivity[] importActivities = bulkExport.bulkExport();

        int lastIndex;
        for (lastIndex = importActivities.length - 1; lastIndex >= 0 && importActivities[lastIndex] == null; lastIndex--) {
            // walk to first non-null
        }

        for (int index = 0; index <= lastIndex; index++) {
            MiruInternalActivity activity = importActivities[index];
            if (activity != null) {
                set(index, activity);
            }
        }
    }

    @Override
    public MiruInternalActivity[] bulkExport() throws Exception {
        int capacity = indexSize.get();

        //TODO all activities need to fit in memory... sigh.
        //TODO need to "stream" this export/import.
        MiruInternalActivity[] activities = new MiruInternalActivity[capacity];
        for (int i = 0; i < activities.length; i++) {
            activities[i] = get(i);
        }
        return activities;
    }
}
