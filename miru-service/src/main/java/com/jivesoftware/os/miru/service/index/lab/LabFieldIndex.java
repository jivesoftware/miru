package com.jivesoftware.os.miru.service.index.lab;

import com.google.common.primitives.Bytes;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ByteBufferDataInput;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.LABUtils;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.MiruInterner;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.index.MultiIndexTx;
import com.jivesoftware.os.miru.plugin.index.TermIdStream;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * @author jonathan.colt
 */
public class LabFieldIndex<BM extends IBM, IBM> implements MiruFieldIndex<BM, IBM> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider idProvider;
    private final MiruBitmaps<BM, IBM> bitmaps;
    private final TrackError trackError;
    private final boolean atomized;
    private final byte[] bitmapPrefix;
    private final ValueIndex<byte[]>[] bitmapIndexes;
    private final byte[] termPrefix;
    private final ValueIndex<byte[]>[] termIndexes;
    private final int termKeyOffset;
    private final byte[] cardinalityPrefix;
    private final ValueIndex<byte[]>[] cardinalities;
    private final boolean[] hasCardinalities;
    // We could lock on both field + termId for improved hash/striping, but we favor just termId to reduce object creation
    private final StripingLocksProvider<MiruTermId> stripingLocksProvider;
    private final MiruInterner<MiruTermId> termInterner;

    public LabFieldIndex(OrderIdProvider idProvider,
        MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        boolean atomized,
        byte[] bitmapPrefix,
        ValueIndex<byte[]>[] bitmapIndexes,
        byte[] termPrefix,
        ValueIndex<byte[]>[] termIndexes,
        byte[] cardinalityPrefix,
        ValueIndex<byte[]>[] cardinalities,
        boolean[] hasCardinalities,
        StripingLocksProvider<MiruTermId> stripingLocksProvider,
        MiruInterner<MiruTermId> termInterner) throws Exception {

        this.idProvider = idProvider;
        this.bitmaps = bitmaps;
        this.trackError = trackError;
        this.atomized = atomized;
        this.bitmapPrefix = bitmapPrefix;
        this.bitmapIndexes = bitmapIndexes;
        this.termPrefix = termPrefix;
        this.termIndexes = termIndexes;
        this.termKeyOffset = termPrefix.length + 4;
        this.cardinalityPrefix = cardinalityPrefix;
        this.cardinalities = cardinalities;
        this.hasCardinalities = hasCardinalities;
        this.stripingLocksProvider = stripingLocksProvider;
        this.termInterner = termInterner;
    }

    private ValueIndex<byte[]> getBitmapIndex(int fieldId) {
        return bitmapIndexes[fieldId % bitmapIndexes.length];
    }

    private ValueIndex<byte[]> getTermIndex(int fieldId) {
        return termIndexes[fieldId % termIndexes.length];
    }

    private ValueIndex<byte[]> getCardinalityIndex(int fieldId) {
        return cardinalities[fieldId % cardinalities.length];
    }

    @Override
    public void set(int fieldId, MiruTermId termId, int[] ids, long[] counts, StackBuffer stackBuffer) throws Exception {
        getIndex("set", fieldId, termId).set(stackBuffer, ids);
        mergeCardinalities(fieldId, termId, ids, counts);
    }

    @Override
    public void setIfEmpty(int fieldId, MiruTermId termId, int id, long count, StackBuffer stackBuffer) throws Exception {
        if (getIndex("setIfEmpty", fieldId, termId).setIfEmpty(stackBuffer, id)) {
            mergeCardinalities(fieldId, termId, new int[] { id }, new long[] { count });
        }
    }

    @Override
    public void remove(int fieldId, MiruTermId termId, int[] ids, StackBuffer stackBuffer) throws Exception {
        getIndex("remove", fieldId, termId).remove(stackBuffer, ids);
        mergeCardinalities(fieldId, termId, ids, cardinalities[fieldId] != null ? new long[ids.length] : null);
    }

    @Override
    public void streamTermIdsForField(String name,
        int fieldId,
        List<KeyRange> ranges,
        final TermIdStream termIdStream,
        StackBuffer stackBuffer) throws Exception {
        MutableLong bytes = new MutableLong();
        byte[] fieldIdBytes = FilerIO.intBytes(fieldId);
        if (ranges == null) {
            byte[] from = termIndexPrefixLowerInclusive(fieldIdBytes);
            byte[] to = termIndexPrefixUpperExclusive(fieldIdBytes);
            getTermIndex(fieldId).rangeScan(from, to, (index, key, timestamp, tombstoned, version, payload) -> {
                byte[] keyBytes = key.copy();
                bytes.add(keyBytes.length);
                return termIdStream.stream(termInterner.intern(keyBytes, termKeyOffset, keyBytes.length - termKeyOffset));
            }, true);
        } else {
            for (KeyRange range : ranges) {
                byte[] from = range.getStartInclusiveKey() != null
                    ? termIndexKey(fieldIdBytes, range.getStartInclusiveKey())
                    : termIndexPrefixLowerInclusive(fieldIdBytes);
                byte[] to = range.getStopExclusiveKey() != null
                    ? termIndexKey(fieldIdBytes, range.getStopExclusiveKey())
                    : termIndexPrefixUpperExclusive(fieldIdBytes);
                getTermIndex(fieldId).rangeScan(from, to, (index, key, timestamp, tombstoned, version, payload) -> {
                    byte[] keyBytes = key.copy();
                    bytes.add(keyBytes.length);
                    return termIdStream.stream(termInterner.intern(keyBytes, termKeyOffset, keyBytes.length - termKeyOffset));
                }, true);
            }
        }

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

    private byte[] bitmapIndexKey(byte[] fieldIdBytes, byte[] termIdBytes) {
        if (atomized) {
            byte[] termLength = new byte[2];
            UIO.shortBytes((short) (termIdBytes.length & 0xFFFF), termLength, 0);
            return Bytes.concat(bitmapPrefix, fieldIdBytes, termLength, termIdBytes);
        } else {
            return Bytes.concat(bitmapPrefix, fieldIdBytes, termIdBytes);
        }
    }

    private byte[] termIndexKey(byte[] fieldIdBytes, byte[] termIdBytes) {
        return Bytes.concat(termPrefix, fieldIdBytes, termIdBytes);
    }

    private byte[] termIndexPrefixLowerInclusive(byte[] fieldIdBytes) {
        return Bytes.concat(termPrefix, fieldIdBytes);
    }

    private byte[] termIndexPrefixUpperExclusive(byte[] fieldIdBytes) {
        byte[] bytes = termIndexPrefixLowerInclusive(fieldIdBytes);
        MiruTermComposer.makeUpperExclusive(bytes);
        return bytes;
    }

    private byte[] cardinalityIndexKey(byte[] fieldIdBytes, int id, byte[] termIdBytes) {
        return Bytes.concat(cardinalityPrefix, fieldIdBytes, FilerIO.intBytes(id), termIdBytes);
    }

    private MiruInvertedIndex<BM, IBM> getIndex(String name, int fieldId, MiruTermId termId) throws Exception {
        byte[] fieldIdBytes = FilerIO.intBytes(fieldId);

        return new LabInvertedIndex<>(idProvider,
            bitmaps,
            trackError,
            name,
            fieldId,
            atomized,
            bitmapIndexKey(fieldIdBytes, termId.getBytes()),
            getBitmapIndex(fieldId),
            termIndexKey(fieldIdBytes, termId.getBytes()),
            getTermIndex(fieldId),
            stripingLocksProvider.lock(termId, 0));
    }

    @Override
    public void multiGet(String name, int fieldId, MiruTermId[] termIds, BitmapAndLastId<BM>[] results, StackBuffer stackBuffer) throws Exception {
        byte[] fieldIdBytes = FilerIO.intBytes(fieldId);
        MutableLong bytes = new MutableLong();
        ValueIndex<byte[]> bitmapIndex = getBitmapIndex(fieldId);
        if (atomized) {
            for (int i = 0; i < termIds.length; i++) {
                if (termIds[i] != null) {
                    byte[] termBytes = termIds[i].getBytes();
                    BitmapAndLastId<BM> bitmapAndLastId = new BitmapAndLastId<>();
                    bitmaps.deserializeAtomized(
                        bitmapAndLastId,
                        atomStream -> {
                            byte[] from = bitmapIndexKey(fieldIdBytes, termBytes);
                            byte[] to = LABUtils.prefixUpperExclusive(from);
                            return bitmapIndex.rangeScan(from, to,
                                (index1, key, timestamp, tombstoned, version, payload) -> {
                                    if (payload != null) {
                                        bytes.add(payload.length);
                                        int labKey = LabInvertedIndex.deatomize(key.asByteBuffer());
                                        return atomStream.stream(labKey, new ByteBufferDataInput(payload.asByteBuffer()));
                                    }
                                    return true;
                                },
                                true);
                        });
                    results[i] = bitmapAndLastId.isSet() ? bitmapAndLastId : null;
                }
            }
        } else {
            bitmapIndex.get(
                keyStream -> {
                    for (int i = 0; i < termIds.length; i++) {
                        if (termIds[i] != null) {
                            byte[] key = bitmapIndexKey(fieldIdBytes, termIds[i].getBytes());
                            if (!keyStream.key(i, key, 0, key.length)) {
                                return false;
                            }
                        }
                    }
                    return true;
                },
                (index, key, timestamp, tombstoned, version, payload) -> {
                    if (payload != null) {
                        bytes.add(payload.length);
                        BitmapAndLastId<BM> bitmapAndLastId = new BitmapAndLastId<>();
                        LabInvertedIndex.deserNonAtomized(bitmaps,
                            trackError,
                            payload.asByteBuffer(),
                            bitmapAndLastId);
                        results[index] = bitmapAndLastId.isSet() ? bitmapAndLastId : null;
                    }
                    return true;
                },
                true);
        }
        LOG.inc("count>multiGet>total");
        LOG.inc("count>multiGet>" + name + ">total");
        LOG.inc("count>multiGet>" + name + ">" + fieldId);
        LOG.inc("bytes>multiGet>total", bytes.longValue());
        LOG.inc("bytes>multiGet>" + name + ">total", bytes.longValue());
        LOG.inc("bytes>multiGet>" + name + ">" + fieldId, bytes.longValue());
    }

    @Override
    public void multiGetLastIds(String name, int fieldId, MiruTermId[] termIds, int[] results, StackBuffer stackBuffer) throws Exception {
        byte[] fieldIdBytes = FilerIO.intBytes(fieldId);
        MutableLong bytes = new MutableLong();
        ValueIndex<byte[]> bitmapIndex = getBitmapIndex(fieldId);
        if (atomized) {
            int[] lastId = new int[1];
            for (int i = 0; i < termIds.length; i++) {
                if (termIds[i] != null) {
                    lastId[0] = -1;
                    byte[] from = bitmapIndexKey(fieldIdBytes, termIds[i].getBytes());
                    byte[] to = LABUtils.prefixUpperExclusive(from);
                    bitmapIndex.rangeScan(from, to,
                        (index, key, timestamp, tombstoned, version, payload) -> {
                            if (payload != null) {
                                if (lastId[0] == -1) {
                                    bytes.add(payload.length);
                                    int labKey = LabInvertedIndex.deatomize(key.asByteBuffer());
                                    lastId[0] = LabInvertedIndex.deserLastId(bitmaps, atomized, labKey, payload.asByteBuffer());
                                    if (lastId[0] != -1) {
                                        return false;
                                    }
                                } else {
                                    LOG.warn("Atomized multiGetLastIds failed to halt a range scan");
                                }
                            }
                            return true;
                        },
                        true);
                    results[i] = lastId[0];
                }
            }
        } else {
            bitmapIndex.get(
                keyStream -> {
                    for (int i = 0; i < termIds.length; i++) {
                        if (termIds[i] != null) {
                            byte[] key = bitmapIndexKey(fieldIdBytes, termIds[i].getBytes());
                            if (!keyStream.key(i, key, 0, key.length)) {
                                return false;
                            }
                        }
                    }
                    return true;
                },
                (index, key, timestamp, tombstoned, version, payload) -> {
                    if (payload != null) {
                        bytes.add(payload.length);
                        results[index] = payload.getInt(0);
                    }
                    return true;
                },
                true);
        }

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

        byte[] fieldIdBytes = FilerIO.intBytes(fieldId);
        MutableLong bytes = new MutableLong();
        ValueIndex<byte[]> bitmapIndex = getBitmapIndex(fieldId);
        if (atomized) {
            int[] lastId = new int[1];
            BitmapAndLastId<BM> container = new BitmapAndLastId<>();
            for (int i = 0; i < termIds.length; i++) {
                if (termIds[i] != null) {
                    container.clear();
                    lastId[0] = -1;
                    byte[] termBytes = termIds[i].getBytes();
                    byte[] from = bitmapIndexKey(fieldIdBytes, termBytes);
                    byte[] to = LABUtils.prefixUpperExclusive(from);

                    bitmaps.deserializeAtomized(
                        container,
                        atomStream -> {
                            return bitmapIndex.rangeScan(from, to,
                                (index, key, timestamp, tombstoned, version, payload) -> {
                                    if (payload != null) {
                                        bytes.add(payload.length);
                                        int labKey = LabInvertedIndex.deatomize(key.asByteBuffer());
                                        if (lastId[0] == -1) {
                                            lastId[0] = LabInvertedIndex.deserLastId(bitmaps, atomized, labKey, payload.asByteBuffer());
                                            if (lastId[0] != -1 && lastId[0] < considerIfLastIdGreaterThanN) {
                                                return false;
                                            }
                                        }
                                        return atomStream.stream(labKey, new ByteBufferDataInput(payload.asByteBuffer()));
                                    }
                                    return true;
                                },
                                true);
                        });

                    if (container.isSet() && (considerIfLastIdGreaterThanN < 0 || lastId[0] > considerIfLastIdGreaterThanN)) {
                        indexTx.tx(i, container.getLastId(), container.getBitmap(), null, -1, stackBuffer);
                    }
                }
            }
        } else {
            bitmapIndex.get(
                keyStream -> {
                    for (int i = 0; i < termIds.length; i++) {
                        if (termIds[i] != null) {
                            byte[] key = bitmapIndexKey(fieldIdBytes, termIds[i].getBytes());
                            if (!keyStream.key(i, key, 0, key.length)) {
                                return false;
                            }
                        }
                    }
                    return true;
                },
                (index, key, timestamp, tombstoned, version, payload) -> {
                    if (payload != null) {
                        bytes.add(payload.length);
                        int lastId = payload.getInt(0);
                        if (considerIfLastIdGreaterThanN < 0 || lastId > considerIfLastIdGreaterThanN) {
                            indexTx.tx(index, lastId, null, new ByteBufferBackedFiler(payload.asByteBuffer()), LabInvertedIndex.LAST_ID_LENGTH, stackBuffer);
                        }
                    }
                    return true;
                }, true);
        }

        LOG.inc("count>multiTxIndex>total");
        LOG.inc("count>multiTxIndex>" + name + ">total");
        LOG.inc("count>multiTxIndex>" + name + ">" + fieldId);
        LOG.inc("bytes>multiTxIndex>total", bytes.longValue());
        LOG.inc("bytes>multiTxIndex>" + name + ">total", bytes.longValue());
        LOG.inc("bytes>multiTxIndex>" + name + ">" + fieldId, bytes.longValue());
    }

    @Override
    public long getCardinality(int fieldId, MiruTermId termId, int id, StackBuffer stackBuffer) throws Exception {
        if (hasCardinalities[fieldId]) {
            byte[] fieldIdBytes = FilerIO.intBytes(fieldId);
            long[] count = { 0 };
            byte[] cardinalityIndexKey = cardinalityIndexKey(fieldIdBytes, id, termId.getBytes());
            getCardinalityIndex(fieldId).get((streamKeys) -> streamKeys.key(0, cardinalityIndexKey, 0, cardinalityIndexKey.length),
                (index, key, timestamp, tombstoned, version, payload) -> {
                    if (payload != null && !tombstoned) {
                        count[0] = payload.getLong(0);
                    }
                    return false;
                }, true);
            return count[0];
        }
        return -1;
    }

    @Override
    public long[] getCardinalities(int fieldId, MiruTermId termId, int[] ids, StackBuffer stackBuffer) throws Exception {
        long[] counts = new long[ids.length];
        if (hasCardinalities[fieldId]) {
            byte[] fieldIdBytes = FilerIO.intBytes(fieldId);
            ValueIndex<byte[]> cardinalityIndex = getCardinalityIndex(fieldId);

            cardinalityIndex.get(
                (stream) -> {
                    for (int i = 0; i < ids.length; i++) {
                        if (ids[i] >= 0) {
                            byte[] cardinalityIndexKey = cardinalityIndexKey(fieldIdBytes, ids[i], termId.getBytes());
                            if (!stream.key(i, cardinalityIndexKey, 0, cardinalityIndexKey.length)) {
                                return false;
                            }
                        }
                    }
                    return true;
                },
                (index, key, timestamp, tombstoned, version, payload) -> {
                    if (payload != null && !tombstoned) {
                        counts[index] = payload.getLong(0);
                    }
                    return true;
                },
                true);
        } else {
            Arrays.fill(counts, -1);
        }
        return counts;
    }

    @Override
    public long getGlobalCardinality(int fieldId, MiruTermId termId, StackBuffer stackBuffer) throws Exception {
        return getCardinality(fieldId, termId, -1, stackBuffer);
    }

    private void mergeCardinalities(int fieldId, MiruTermId termId, int[] ids, long[] counts) throws Exception {
        if (hasCardinalities[fieldId] && counts != null) {
            byte[] fieldBytes = FilerIO.intBytes(fieldId);
            ValueIndex<byte[]> cardinalityIndex = getCardinalityIndex(fieldId);

            long[] merge = new long[counts.length];
            long delta = 0;
            //System.arraycopy(counts, 0, merged, 0, counts.length);

            cardinalityIndex.get(
                keyStream -> {
                    for (int i = 0; i < ids.length; i++) {
                        byte[] key = cardinalityIndexKey(fieldBytes, ids[i], termId.getBytes());
                        if (!keyStream.key(i, key, 0, key.length)) {
                            return false;
                        }
                    }
                    return true;
                },
                (index, key, timestamp, tombstoned, version, payload) -> {
                    if (payload != null && !tombstoned) {
                        merge[index] = payload.getLong(0);
                    }
                    return false;
                },
                true);

            for (int i = 0; i < ids.length; i++) {
                delta += counts[i] - merge[i];
            }

            long[] globalCount = { 0 };
            byte[] cardinalityIndexKey = cardinalityIndexKey(fieldBytes, -1, termId.getBytes());
            cardinalityIndex.get(
                (keyStream) -> keyStream.key(0, cardinalityIndexKey, 0, cardinalityIndexKey.length),
                (index, key, timestamp, tombstoned, version, payload) -> {
                    if (payload != null && !tombstoned) {
                        globalCount[0] = payload.getLong(0);
                    }
                    return false;
                },
                true);
            globalCount[0] += delta;

            long timestamp = System.currentTimeMillis();
            long version = idProvider.nextId();
            cardinalityIndex.append(
                valueStream -> {
                    for (int i = 0; i < ids.length; i++) {
                        byte[] key = cardinalityIndexKey(fieldBytes, ids[i], termId.getBytes());
                        if (!valueStream.stream(-1, key, timestamp, false, version, UIO.longBytes(counts[i]))) {
                            return false;
                        }
                    }

                    byte[] globalKey = cardinalityIndexKey(fieldBytes, -1, termId.getBytes());
                    valueStream.stream(-1, globalKey, timestamp, false, version, UIO.longBytes(globalCount[0]));
                    return true;
                },
                true,
                new BolBuffer(),
                new BolBuffer());
        }
    }

}
