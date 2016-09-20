package com.jivesoftware.os.miru.service.index.lab;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ByteBufferDataInput;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.LABUtils;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.IndexTx;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * @author jonathan
 */
public class LabInvertedIndex<BM extends IBM, IBM> implements MiruInvertedIndex<BM, IBM> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static final int LAST_ID_LENGTH = 4;

    private final OrderIdProvider idProvider;
    private final MiruBitmaps<BM, IBM> bitmaps;
    private final TrackError trackError;
    private final String name;
    private final int fieldId;
    private final boolean atomized;
    private final byte[] bitmapKeyBytes;
    private final ValueIndex bitmapIndex;
    private final byte[] termKeyBytes;
    private final ValueIndex termIndex;
    private final Object mutationLock;
    private volatile int lastId = Integer.MIN_VALUE;

    public LabInvertedIndex(OrderIdProvider idProvider,
        MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        String name,
        int fieldId,
        boolean atomized,
        byte[] bitmapKeyBytes,
        ValueIndex bitmapIndex,
        byte[] termKeyBytes,
        ValueIndex termIndex,
        Object mutationLock) {

        this.idProvider = idProvider;
        this.bitmaps = bitmaps;
        this.trackError = trackError;
        this.name = name;
        this.fieldId = fieldId;
        this.atomized = atomized;
        this.bitmapKeyBytes = Preconditions.checkNotNull(bitmapKeyBytes);
        this.bitmapIndex = Preconditions.checkNotNull(bitmapIndex);
        this.termKeyBytes = termKeyBytes;
        this.termIndex = termIndex;
        this.mutationLock = mutationLock;
    }

    @Override
    public Optional<BM> getIndex(StackBuffer stackBuffer) throws Exception {
        MutableLong bytes = new MutableLong();
        Optional<BM> index = getIndexInternal(null, bytes).transform(input -> input.bitmap);
        LOG.inc("count>getIndex>total");
        LOG.inc("count>getIndex>" + name + ">total");
        LOG.inc("count>getIndex>" + name + ">" + fieldId);
        LOG.inc("bytes>getIndex>total", bytes.longValue());
        LOG.inc("bytes>getIndex>" + name + ">total", bytes.longValue());
        LOG.inc("bytes>getIndex>" + name + ">" + fieldId, bytes.longValue());
        return index;
    }

    private Optional<BitmapAndLastId<BM>> getIndexInternal(int[] keys, MutableLong bytes) throws Exception {

        @SuppressWarnings("unchecked")
        BitmapAndLastId<BM> bitmapAndLastId;
        if (atomized) {
            List<LabKeyBytes> labKeyBytes = Lists.newArrayList();
            if (keys != null) {
                bitmapIndex.get(
                    keyStream -> {
                        for (int key : keys) {
                            byte[] keyBytes = atomize(bitmapKeyBytes, key);
                            if (!keyStream.key(-1, keyBytes, 0, keyBytes.length)) {
                                return false;
                            }
                        }
                        return true;
                    },
                    (index, key, timestamp, tombstoned, version, payload) -> {
                        if (payload != null) {
                            labKeyBytes.add(new LabKeyBytes(deatomize(key.asByteBuffer()), ByteBuffer.wrap(payload.copy())));
                            bytes.add(payload.length);
                        }
                        return true;
                    },
                    true);
                LOG.inc("atomized>getKeys>calls");
                LOG.inc("atomized>getKeys>keys", keys.length);
                LOG.inc("atomized>getKeys>atoms", labKeyBytes.size());
            } else {
                byte[] from = bitmapKeyBytes;
                byte[] to = LABUtils.prefixUpperExclusive(bitmapKeyBytes);
                bitmapIndex.rangeScan(from, to,
                    (index, key, timestamp, tombstoned, version, payload) -> {
                        if (payload != null) {
                            labKeyBytes.add(new LabKeyBytes(deatomize(key.asByteBuffer()), ByteBuffer.wrap(payload.copy())));
                            bytes.add(payload.length);
                        }
                        return true;
                    },
                    true);
                LOG.inc("atomized>getRange>calls");
                LOG.inc("atomized>getRange>atoms", labKeyBytes.size());
            }
            Collections.reverse(labKeyBytes);
            bitmapAndLastId = labKeyBytes.isEmpty() ? null : deser(bitmaps, trackError, atomized, labKeyBytes);
        } else {
            @SuppressWarnings("unchecked")
            BitmapAndLastId<BM>[] bali = new BitmapAndLastId[1];
            bitmapIndex.get((keyStream) -> keyStream.key(0, bitmapKeyBytes, 0, bitmapKeyBytes.length),
                (index, key, timestamp, tombstoned, version, payload) -> {
                    if (payload != null) {
                        bali[0] = deser(bitmaps, trackError, atomized, Collections.singletonList(new LabKeyBytes(-1, ByteBuffer.wrap(payload.copy()))));
                        bytes.add(payload.length);
                    }
                    return true;
                }, true);
            bitmapAndLastId = bali[0];
        }

        if (bitmapAndLastId != null) {
            LOG.inc("get>hit");
            if (lastId == Integer.MIN_VALUE) {
                lastId = bitmapAndLastId.lastId;
            }
            return Optional.of(bitmapAndLastId);
        } else {
            LOG.inc("get>miss");
            lastId = -1;
            return Optional.absent();
        }
    }

    @Override
    public <R> R txIndex(IndexTx<R, IBM> tx, StackBuffer stackBuffer) throws Exception {
        MutableLong bytes = new MutableLong();
        @SuppressWarnings("unchecked")
        R[] result = (R[]) new Object[1];
        bitmapIndex.get(
            (keyStream) -> keyStream.key(0, bitmapKeyBytes, 0, bitmapKeyBytes.length),
            (index, key, timestamp, tombstoned, version, payload) -> {
                try {
                    if (payload != null) {
                        bytes.add(payload.length);
                        if (payload.length < LAST_ID_LENGTH + 4) {
                            result[0] = tx.tx(null, null, -1, null);
                            return false;
                        } else {
                            result[0] = tx.tx(null, new ByteBufferBackedFiler(payload.asByteBuffer()), LAST_ID_LENGTH, stackBuffer);
                            return false;
                        }
                    } else {
                        result[0] = tx.tx(null, null, -1, null);
                        return false;
                    }
                } catch (Exception e) {
                    throw new IOException(e);
                }
            },
            true
        );

        LOG.inc("count>txIndex>total");
        LOG.inc("count>txIndex>" + name + ">total");
        LOG.inc("count>txIndex>" + name + ">" + fieldId);
        LOG.inc("bytes>txIndex>total", bytes.longValue());
        LOG.inc("bytes>txIndex>" + name + ">total", bytes.longValue());
        LOG.inc("bytes>txIndex>" + name + ">" + fieldId, bytes.longValue());
        return result[0];
    }

    public static <BM extends IBM, IBM> BitmapAndLastId<BM> deser(MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        boolean atomized,
        List<LabKeyBytes> labKeyBytes) throws IOException {

        if (atomized) {
            DataInput[] dataInputs = new DataInput[labKeyBytes.size()];
            int[] keys = new int[dataInputs.length];
            for (int i = 0; i < dataInputs.length; i++) {
                LabKeyBytes kb = labKeyBytes.get(i);
                keys[i] = kb.key;
                dataInputs[i] = new ByteBufferDataInput(kb.byteBuffer);
            }
            try {
                return bitmaps.deserializeAtomized(dataInputs, keys);
            } catch (Exception e) {
                trackError.error("Failed to deserialize atomized bitmap, keys=" + labKeyBytes.size());
                throw new IOException("Failed to deserialize atomized", e);
            }
        } else {
            ByteBuffer byteBuffer = labKeyBytes.get(0).byteBuffer;
            if (byteBuffer.capacity() > LAST_ID_LENGTH + 4) {
                int lastId = byteBuffer.getInt();
                byteBuffer.position(LAST_ID_LENGTH);
                DataInput dataInput = new ByteBufferDataInput(byteBuffer);
                try {
                    return new BitmapAndLastId<>(bitmaps.deserialize(dataInput), lastId);
                } catch (Exception e) {
                    trackError.error("Failed to deserialize a bitmap, length=" + byteBuffer.capacity());
                    throw new IOException("Failed to deserialize", e);
                }
            }
        }
        return null;
    }

    public static <BM extends IBM, IBM> int deserLastId(MiruBitmaps<BM, IBM> bitmaps,
        boolean atomized,
        int key,
        ByteBuffer byteBuffer) throws IOException {

        byteBuffer.clear();
        if (atomized) {
            return bitmaps.lastIdAtomized(new ByteBufferDataInput(byteBuffer), key);
        } else {
            if (byteBuffer.capacity() > LAST_ID_LENGTH + 4) {
                return byteBuffer.getInt();
            }
        }
        return -1;
    }

    public static byte[] atomize(byte[] indexKeyBytes, int key) {
        byte[] atom = new byte[indexKeyBytes.length + 2];
        System.arraycopy(indexKeyBytes, 0, atom, 0, indexKeyBytes.length);
        short reversed = (short) ((0xFFFF - key) & 0xFFFF);
        atom[atom.length - 2] = (byte) (reversed >>> 8);
        atom[atom.length - 1] = (byte) reversed;
        return atom;
    }

    public static int deatomize(byte[] key) {
        int v = 0;
        v |= (key[key.length - 2] & 0xFF);
        v <<= 8;
        v |= (key[key.length - 1] & 0xFF);
        return 0xFFFF - v;
    }

    public static int deatomize(ByteBuffer key) {
        key.clear();
        int v = 0;
        v |= (key.get(key.capacity() - 2) & 0xFF);
        v <<= 8;
        v |= (key.get(key.capacity() - 1) & 0xFF);
        return 0xFFFF - v;
    }

    private BM getOrCreateIndex(int[] keys) throws Exception {
        MutableLong bytes = new MutableLong();
        Optional<BitmapAndLastId<BM>> index = getIndexInternal(keys, bytes);
        BM bitmap = index.isPresent() ? index.get().bitmap : bitmaps.create();
        LOG.inc("count>getOrCreateIndex>total");
        LOG.inc("count>getOrCreateIndex>" + name + ">total");
        LOG.inc("count>getOrCreateIndex>" + name + ">" + fieldId);
        LOG.inc("bytes>getOrCreateIndex>total", bytes.longValue());
        LOG.inc("bytes>getOrCreateIndex>" + name + ">total", bytes.longValue());
        LOG.inc("bytes>getOrCreateIndex>" + name + ">" + fieldId, bytes.longValue());
        return bitmap;
    }

    private static <BM extends IBM, IBM> long serializedSizeInBytes(MiruBitmaps<BM, IBM> bitmaps, IBM index) {
        return LAST_ID_LENGTH + bitmaps.serializedSizeInBytes(index);
    }

    private byte[][] keyBytes(int[] keys, IBM index) throws Exception {
        byte[][] bytes;
        if (atomized) {
            long[] sizes = bitmaps.serializeAtomizedSizeInBytes(index, keys);
            ByteArrayDataOutput[] dataOutputs = new ByteArrayDataOutput[keys.length];
            for (int i = 0; i < keys.length; i++) {
                dataOutputs[i] = sizes[i] < 0 ? null : ByteStreams.newDataOutput((int) sizes[i]);
            }
            bitmaps.serializeAtomized(index, keys, dataOutputs);
            bytes = new byte[keys.length][];
            for (int i = 0; i < keys.length; i++) {
                bytes[i] = dataOutputs[i] == null ? null : dataOutputs[i].toByteArray();
            }
        } else {
            long size = serializedSizeInBytes(bitmaps, index);
            ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput((int) size);
            dataOutput.write(FilerIO.intBytes(lastId));
            bitmaps.serialize(index, dataOutput);
            bytes = new byte[][] { dataOutput.toByteArray() };
        }
        return bytes;
    }

    private void setIndex(int[] keys, IBM index) throws Exception {
        byte[][] bytes = keyBytes(keys, index);

        long timestamp = System.currentTimeMillis();
        long version = idProvider.nextId();
        if (termIndex != null) {
            boolean[] exists = { false };
            termIndex.get(keyStream -> keyStream.key(-1, termKeyBytes, 0, termKeyBytes.length),
                (index1, key, timestamp1, tombstoned, version1, payload) -> {
                    exists[0] = timestamp1 > 0 && !tombstoned;
                    return true;
                }, false);
            if (!exists[0]) {
                termIndex.append(
                    stream -> {
                        if (!stream.stream(-1, termKeyBytes, timestamp, false, version, null)) {
                            return false;
                        }
                        return true;
                    },
                    true,
                    new BolBuffer(),
                    new BolBuffer());
            }
        }

        bitmapIndex.append(
            stream -> {
                if (atomized) {
                    for (int i = 0; i < keys.length; i++) {
                        if (!stream.stream(-1, atomize(bitmapKeyBytes, keys[i]), timestamp, false, version, bytes[i])) {
                            return false;
                        }
                    }
                } else {
                    if (!stream.stream(-1, bitmapKeyBytes, timestamp, false, version, bytes[0])) {
                        return false;
                    }
                }
                return true;
            },
            true,
            new BolBuffer(),
            new BolBuffer());

        lastId = bitmaps.lastSetBit(index);

        int bytesWritten = 0;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] != null) {
                bytesWritten += bytes[i].length;
            }
        }
        LOG.inc("count>set>total");
        LOG.inc("count>set>" + name + ">total");
        LOG.inc("count>set>" + name + ">" + fieldId);
        LOG.inc("bytes>set>total", bytesWritten);
        LOG.inc("bytes>set>" + name + ">total", bytesWritten);
        LOG.inc("bytes>set>" + name + ">" + fieldId, bytesWritten);
        if (atomized) {
            LOG.inc("atomized>set>calls");
            LOG.inc("atomized>set>atoms", keys == null ? 0 : keys.length);
        }
    }

    private int[] keysFromIds(int... ids) {
        TIntSet keySet = new TIntHashSet();
        for (int id : ids) {
            keySet.add(bitmaps.key(id));
        }
        int[] keys = keySet.toArray();
        Arrays.sort(keys);
        return keys;
    }

    @Override
    public void remove(StackBuffer stackBuffer, int... ids) throws Exception {
        synchronized (mutationLock) {
            int[] keys = keysFromIds(ids);
            BM index = getOrCreateIndex(keys);
            BM r = bitmaps.remove(index, ids);

            setIndex(keys, r);
        }
    }

    @Override
    public void set(StackBuffer stackBuffer, int... ids) throws Exception {
        if (ids.length == 0) {
            return;
        }
        synchronized (mutationLock) {
            int[] keys = keysFromIds(ids);
            BM index = getOrCreateIndex(keys);
            BM r = bitmaps.set(index, ids);

            setIndex(keys, r);
        }
    }

    @Override
    public boolean setIfEmpty(StackBuffer stackBuffer, int id) throws Exception {
        synchronized (mutationLock) {
            int lastId = lastId(stackBuffer);
            if (lastId < 0) {
                set(stackBuffer, id);
                return true;
            }
        }
        return false;
    }

    @Override
    public int lastId(StackBuffer stackBuffer) throws Exception {
        if (lastId == Integer.MIN_VALUE) {
            MutableLong bytes = new MutableLong();
            synchronized (mutationLock) {
                int[] id = { -1 };
                if (atomized) {
                    byte[] from = bitmapKeyBytes;
                    byte[] to = LABUtils.prefixUpperExclusive(bitmapKeyBytes);
                    bitmapIndex.rangeScan(from, to,
                        (index, key, timestamp, tombstoned, version, payload) -> {
                            if (payload != null) {
                                if (id[0] == -1) {
                                    bytes.add(payload.length);
                                    int labKey = LabInvertedIndex.deatomize(key.asByteBuffer());
                                    id[0] = LabInvertedIndex.deserLastId(bitmaps, atomized, labKey, payload.asByteBuffer());
                                    if (id[0] != -1) {
                                        return false;
                                    }
                                } else {
                                    LOG.warn("Atomized multiGetLastIds failed to halt a range scan");
                                }
                            }
                            return true;
                        },
                        true);
                } else {
                    bitmapIndex.get(
                        (keyStream) -> keyStream.key(0, bitmapKeyBytes, 0, bitmapKeyBytes.length),
                        (index, key, timestamp, tombstoned, version, payload) -> {
                            if (payload != null) {
                                bytes.add(payload.length);
                                id[0] = payload.getInt(0);
                            }
                            return true;
                        },
                        true
                    );
                }
                lastId = id[0];
            }
            LOG.inc("count>lastId>total");
            LOG.inc("count>lastId>" + name + ">total");
            LOG.inc("count>lastId>" + name + ">" + fieldId);
            LOG.inc("bytes>lastId>total", bytes.longValue());
            LOG.inc("bytes>lastId>" + name + ">total", bytes.longValue());
            LOG.inc("bytes>lastId>" + name + ">" + fieldId, bytes.longValue());
        }
        return lastId;
    }

    @Override
    public void andNot(IBM mask, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            int[] keys = bitmaps.keys(mask);
            BM index = getOrCreateIndex(keys);
            BM r = bitmaps.andNot(index, mask);
            setIndex(keys, r);
        }
    }

    @Override
    public void or(IBM mask, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            int[] keys = bitmaps.keys(mask);
            BM index = getOrCreateIndex(keys);
            BM r = bitmaps.or(Arrays.asList(index, mask));
            setIndex(keys, r);
        }
    }

    @Override
    public void andNotToSourceSize(List<IBM> masks, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            TIntSet keySet = new TIntHashSet();
            for (IBM mask : masks) {
                keySet.addAll(bitmaps.keys(mask));
            }
            int[] keys = keySet.toArray();
            Arrays.sort(keys);
            BM index = getOrCreateIndex(keys);
            BM andNot = bitmaps.andNotToSourceSize(index, masks);
            setIndex(keys, andNot);
        }
    }

    @Override
    public void orToSourceSize(IBM mask, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            int[] keys = bitmaps.keys(mask);
            BM index = getOrCreateIndex(keys);
            BM or = bitmaps.orToSourceSize(index, mask);
            setIndex(keys, or);
        }
    }

}
