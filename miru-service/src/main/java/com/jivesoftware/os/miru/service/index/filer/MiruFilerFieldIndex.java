package com.jivesoftware.os.miru.service.index.filer;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.map.MapContext;
import com.jivesoftware.os.filer.io.map.MapStore;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.MiruInterner;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MultiIndexTx;
import com.jivesoftware.os.miru.plugin.index.TermIdStream;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * @author jonathan
 */
public class MiruFilerFieldIndex<BM extends IBM, IBM> implements MiruFieldIndex<BM, IBM> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruBitmaps<BM, IBM> bitmaps;
    private final TrackError trackError;
    private final KeyedFilerStore<Long, Void>[] indexes;
    private final KeyedFilerStore<Integer, MapContext>[] cardinalities;
    // We could lock on both field + termId for improved hash/striping, but we favor just termId to reduce object creation
    private final StripingLocksProvider<MiruTermId> stripingLocksProvider;
    private final MiruInterner<MiruTermId> termInterner;

    public MiruFilerFieldIndex(MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        KeyedFilerStore<Long, Void>[] indexes,
        KeyedFilerStore<Integer, MapContext>[] cardinalities,
        StripingLocksProvider<MiruTermId> stripingLocksProvider,
        MiruInterner<MiruTermId> termInterner) throws Exception {
        this.bitmaps = bitmaps;
        this.trackError = trackError;
        this.indexes = indexes;
        this.cardinalities = cardinalities;
        this.stripingLocksProvider = stripingLocksProvider;
        this.termInterner = termInterner;
    }

    @Override
    public void set(MiruFieldDefinition fieldDefinition, MiruTermId termId, int[] ids, long[] counts, StackBuffer stackBuffer) throws Exception {
        getIndex("set", fieldDefinition.fieldId, termId).set(stackBuffer, ids);
        mergeCardinalities(fieldDefinition, termId, ids, counts, stackBuffer);
    }

    @Override
    public void setIfEmpty(MiruFieldDefinition fieldDefinition, MiruTermId termId, int id, long count, StackBuffer stackBuffer) throws Exception {
        if (getIndex("setIfEmpty", fieldDefinition.fieldId, termId).setIfEmpty(stackBuffer, id)) {
            mergeCardinalities(fieldDefinition, termId, new int[] { id }, new long[] { count }, stackBuffer);
        }
    }

    @Override
    public void remove(MiruFieldDefinition fieldDefinition, MiruTermId termId, int[] ids, StackBuffer stackBuffer) throws Exception {
        getIndex("remove", fieldDefinition.fieldId, termId).remove(stackBuffer, ids);
        mergeCardinalities(fieldDefinition, termId, ids, cardinalities[fieldDefinition.fieldId] != null ? new long[ids.length] : null, stackBuffer);
    }

    @Override
    public void streamTermIdsForField(String name,
        int fieldId,
        List<KeyRange> ranges,
        final TermIdStream termIdStream,
        StackBuffer stackBuffer) throws Exception {
        MutableLong bytes = new MutableLong();
        indexes[fieldId].streamKeys(ranges, rawKey -> {
            try {
                bytes.add(rawKey.length);
                return termIdStream.stream(termInterner.intern(rawKey));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, stackBuffer);
        LOG.inc("count>streamTermIdsForField>total");
        LOG.inc("count>streamTermIdsForField>" + name + ">total");
        LOG.inc("count>streamTermIdsForField>" + name + ">" + fieldId);
        LOG.inc("bytes>streamTermIdsForField>total", bytes.longValue());
        LOG.inc("bytes>streamTermIdsForField>" + name + ">total", bytes.longValue());
        LOG.inc("bytes>streamTermIdsForField>" + name + ">" + fieldId, bytes.longValue());
    }

    @Override
    public MiruInvertedIndex<BM, IBM> get(String name, int fieldId, MiruTermId termId) throws Exception {
        return getIndex(name, fieldId, termId);
    }

    @Override
    public MiruInvertedIndex<BM, IBM> getOrCreateInvertedIndex(String name, int fieldId, MiruTermId term) throws Exception {
        return getIndex(name, fieldId, term);
    }

    private MiruInvertedIndex<BM, IBM> getIndex(String name, int fieldId, MiruTermId termId) throws Exception {
        return new MiruFilerInvertedIndex<>(bitmaps, trackError, name, fieldId, termId.getBytes(), indexes[fieldId], stripingLocksProvider.lock(termId, 0));
    }

    @Override
    public void multiGet(String name, int fieldId, MiruTermId[] termIds, BitmapAndLastId<BM>[] results, StackBuffer stackBuffer) throws Exception {
        byte[][] termIdBytes = new byte[termIds.length][];
        for (int i = 0; i < termIds.length; i++) {
            if (termIds[i] != null) {
                termIdBytes[i] = termIds[i].getBytes();
            }
        }
        MutableLong bytes = new MutableLong();
        indexes[fieldId].readEach(termIdBytes, null, (monkey, filer, _stackBuffer, lock, index) -> {
            if (filer != null) {
                bytes.add(filer.length());
                BitmapAndLastId<BM> container = new BitmapAndLastId<>();
                MiruFilerInvertedIndex.deser(bitmaps, trackError, filer, -1, container, _stackBuffer);
                if (container.isSet()) {
                    return container;
                }
            }
            return null;
        }, results, stackBuffer);
        LOG.inc("count>multiGet>total");
        LOG.inc("count>multiGet>" + name + ">total");
        LOG.inc("count>multiGet>" + name + ">" + fieldId);
        LOG.inc("bytes>multiGet>total", bytes.longValue());
        LOG.inc("bytes>multiGet>" + name + ">total", bytes.longValue());
        LOG.inc("bytes>multiGet>" + name + ">" + fieldId, bytes.longValue());
    }

    @Override
    public void multiGetLastIds(String name, int fieldId, MiruTermId[] termIds, int[] results, StackBuffer stackBuffer) throws Exception {
        byte[][] termIdBytes = new byte[termIds.length][];
        for (int i = 0; i < termIds.length; i++) {
            if (termIds[i] != null) {
                termIdBytes[i] = termIds[i].getBytes();
            }
        }
        MutableLong bytes = new MutableLong();
        indexes[fieldId].readEach(termIdBytes, null, (monkey, filer, _stackBuffer, lock, index) -> {
            if (filer != null) {
                bytes.add(4);
                results[index] = MiruFilerInvertedIndex.deserLastId(filer);
            }
            return null;
        }, new Void[results.length], stackBuffer);
        LOG.inc("count>multiGetLastIds>total");
        LOG.inc("count>multiGetLastIds>" + name + ">total");
        LOG.inc("count>multiGetLastIds>" + name + ">" + fieldId);
        LOG.inc("bytes>multiGetLastIds>total", bytes.longValue());
        LOG.inc("bytes>multiGetLastIds>" + name + ">total", bytes.longValue());
        LOG.inc("bytes>multiGetLastIds>" + name + ">" + fieldId, bytes.longValue());
    }

    @Override
    public void multiTxIndex(String name,
        int fieldId,
        MiruTermId[] termIds,
        int considerIfLastIdGreaterThanN,
        StackBuffer stackBuffer,
        MultiIndexTx<IBM> indexTx) throws Exception {

        byte[][] termIdBytes = new byte[termIds.length][];
        for (int i = 0; i < termIds.length; i++) {
            if (termIds[i] != null) {
                termIdBytes[i] = termIds[i].getBytes();
            }
        }
        MutableLong bytes = new MutableLong();
        indexes[fieldId].readEach(termIdBytes, null, (monkey, filer, _stackBuffer, lock, index) -> {
            if (filer != null) {
                try {
                    bytes.add(filer.length());
                    int lastId = filer.readInt();
                    filer.seek(0);
                    if (considerIfLastIdGreaterThanN < 0 || lastId > considerIfLastIdGreaterThanN) {
                        indexTx.tx(index, lastId, null, filer, MiruFilerInvertedIndex.LAST_ID_LENGTH, _stackBuffer);
                    }
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
            return null;
        }, new Void[termIds.length], stackBuffer);
        LOG.inc("count>multiTxIndex>total");
        LOG.inc("count>multiTxIndex>" + name + ">total");
        LOG.inc("count>multiTxIndex>" + name + ">" + fieldId);
        LOG.inc("bytes>multiTxIndex>total", bytes.longValue());
        LOG.inc("bytes>multiTxIndex>" + name + ">total", bytes.longValue());
        LOG.inc("bytes>multiTxIndex>" + name + ">" + fieldId, bytes.longValue());
    }

    @Override
    public long getCardinality(MiruFieldDefinition fieldDefinition, MiruTermId termId, int id, StackBuffer stackBuffer) throws Exception {
        int fieldId = fieldDefinition.fieldId;
        if (cardinalities[fieldId] != null) {
            Long count = cardinalities[fieldId].read(termId.getBytes(), null, (monkey, filer, _stackBuffer, lock) -> {
                if (filer != null) {
                    synchronized (lock) {
                        byte[] payload = MapStore.INSTANCE.getPayload(filer, monkey, FilerIO.intBytes(id), _stackBuffer);
                        if (payload != null) {
                            return FilerIO.bytesLong(payload);
                        }
                    }
                }
                return null;
            }, stackBuffer);
            return count != null ? count : 0;
        }
        return -1;
    }

    @Override
    public long[] getCardinalities(MiruFieldDefinition fieldDefinition, MiruTermId termId, int[] ids, StackBuffer stackBuffer) throws Exception {
        int fieldId = fieldDefinition.fieldId;
        long[] counts = new long[ids.length];
        if (cardinalities[fieldId] != null) {
            cardinalities[fieldId].read(termId.getBytes(), null, (monkey, filer, _stackBuffer, lock) -> {
                if (filer != null) {
                    synchronized (lock) {
                        for (int i = 0; i < ids.length; i++) {
                            if (ids[i] >= 0) {
                                byte[] payload = MapStore.INSTANCE.getPayload(filer, monkey, FilerIO.intBytes(ids[i]), _stackBuffer);
                                if (payload != null) {
                                    counts[i] = FilerIO.bytesLong(payload);
                                }
                            }
                        }
                    }
                }
                return null;
            }, stackBuffer);
        } else {
            Arrays.fill(counts, -1);
        }
        return counts;
    }

    @Override
    public long getGlobalCardinality(MiruFieldDefinition fieldDefinition, MiruTermId termId, StackBuffer stackBuffer) throws Exception {
        return getCardinality(fieldDefinition, termId, -1, stackBuffer);
    }

    private void mergeCardinalities(MiruFieldDefinition fieldDefinition,
        MiruTermId termId,
        int[] ids,
        long[] counts,
        StackBuffer stackBuffer) throws Exception {

        int fieldId = fieldDefinition.fieldId;
        if (cardinalities[fieldId] != null && counts != null) {
            cardinalities[fieldId].readWriteAutoGrow(termId.getBytes(), ids.length, (monkey, filer, _stackBuffer, lock) -> {
                synchronized (lock) {
                    long delta = 0;
                    for (int i = 0; i < ids.length; i++) {
                        byte[] key = FilerIO.intBytes(ids[i]);
                        long keyHash = MapStore.INSTANCE.hash(key, 0, key.length);
                        byte[] payload = MapStore.INSTANCE.getPayload(filer, monkey, keyHash, key, stackBuffer);
                        long existing = payload != null ? FilerIO.bytesLong(payload) : 0;
                        MapStore.INSTANCE.add(filer, monkey, (byte) 1, keyHash, key, FilerIO.longBytes(counts[i]), _stackBuffer);
                        delta += counts[i] - existing;
                    }

                    byte[] globalKey = FilerIO.intBytes(-1);
                    byte[] globalPayload = MapStore.INSTANCE.getPayload(filer, monkey, globalKey, _stackBuffer);
                    long globalExisting = globalPayload != null ? FilerIO.bytesLong(globalPayload) : 0;
                    MapStore.INSTANCE.add(filer, monkey, (byte) 1, globalKey, FilerIO.longBytes(globalExisting + delta), _stackBuffer);
                }
                return null;
            }, stackBuffer);
        }
    }

}
