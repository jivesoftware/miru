package com.jivesoftware.os.miru.service.index.lab;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
    private final ValueIndex<byte[]> bitmapIndex;
    private final byte[] termKeyBytes;
    private final ValueIndex<byte[]> termIndex;
    private final Object mutationLock;
    private volatile int lastId = Integer.MIN_VALUE;

    public LabInvertedIndex(OrderIdProvider idProvider,
        MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        String name,
        int fieldId,
        boolean atomized,
        byte[] bitmapKeyBytes,
        ValueIndex<byte[]> bitmapIndex,
        byte[] termKeyBytes,
        ValueIndex<byte[]> termIndex,
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
    public void getIndex(BitmapAndLastId<BM> container, StackBuffer stackBuffer) throws Exception {
        MutableLong bytes = new MutableLong();
        getIndexInternal(null, container, bytes);
        LOG.inc("count>getIndex>total");
        LOG.inc("count>getIndex>" + name + ">total");
        LOG.inc("count>getIndex>" + name + ">" + fieldId);
        LOG.inc("bytes>getIndex>total", bytes.longValue());
        LOG.inc("bytes>getIndex>" + name + ">total", bytes.longValue());
        LOG.inc("bytes>getIndex>" + name + ">" + fieldId, bytes.longValue());
    }

    private void getIndexInternal(int[] keys, BitmapAndLastId<BM> container, MutableLong bytes) throws Exception {
        container.clear();
        ReusableByteBufferDataInput in = new ReusableByteBufferDataInput();
        if (atomized) {
            bitmaps.deserializeAtomized(
                container,
                atomStream -> {
                    if (keys != null) {
                        int[] atoms = { 0 };
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
                                    int labKey = deatomize(key.asByteBuffer());
                                    bytes.add(payload.length);
                                    atoms[0]++;
                                    in.setBuffer(payload.asByteBuffer());
                                    return atomStream.stream(labKey, in);
                                }
                                return true;
                            },
                            true);
                        LOG.inc("getIndexInternal>atomized>getKeys>calls");
                        LOG.inc("getIndexInternal>atomized>getKeys>keys", keys.length);
                        LOG.inc("getIndexInternal>atomized>getKeys>atoms", atoms[0]);
                    } else {
                        byte[] from = bitmapKeyBytes;
                        byte[] to = LABUtils.prefixUpperExclusive(bitmapKeyBytes);
                        int[] atoms = { 0 };
                        bitmapIndex.rangeScan(from, to,
                            (index, key, timestamp, tombstoned, version, payload) -> {
                                if (payload != null) {
                                    int labKey = deatomize(key.asByteBuffer());
                                    bytes.add(payload.length);
                                    atoms[0]++;
                                    in.setBuffer(payload.asByteBuffer());
                                    return atomStream.stream(labKey, in);
                                }
                                return true;
                            },
                            true);
                        LOG.inc("getIndexInternal>atomized>getRange>calls");
                        LOG.inc("getIndexInternal>atomized>getRange>atoms", atoms[0]);
                    }
                    return true;
                });
        } else {
            bitmapIndex.get((keyStream) -> keyStream.key(0, bitmapKeyBytes, 0, bitmapKeyBytes.length),
                (index, key, timestamp, tombstoned, version, payload) -> {
                    if (payload != null) {
                        deserNonAtomized(bitmaps, trackError, in, payload.asByteBuffer(), container);
                        bytes.add(payload.length);
                    }
                    return true;
                }, true);
        }

        if (container.isSet()) {
            LOG.inc("get>hit");
            if (lastId == Integer.MIN_VALUE) {
                lastId = container.getLastId();
            }
        } else {
            LOG.inc("get>miss");
            lastId = -1;
        }
    }

    @Override
    public <R> R txIndex(IndexTx<R, IBM> tx, StackBuffer stackBuffer) throws Exception {
        MutableLong bytes = new MutableLong();
        R result;
        if (atomized) {
            BitmapAndLastId<BM> container = new BitmapAndLastId<>();
            ReusableByteBufferDataInput in = new ReusableByteBufferDataInput();
            bitmaps.deserializeAtomized(
                container,
                atomStream -> {
                    byte[] from = bitmapKeyBytes;
                    byte[] to = LABUtils.prefixUpperExclusive(bitmapKeyBytes);
                    int[] atoms = { 0 };
                    bitmapIndex.rangeScan(from, to,
                        (index, key, timestamp, tombstoned, version, payload) -> {
                            if (payload != null) {
                                int labKey = deatomize(key.asByteBuffer());
                                bytes.add(payload.length);
                                atoms[0]++;
                                in.setBuffer(payload.asByteBuffer());
                                return atomStream.stream(labKey, in);
                            }
                            return true;
                        },
                        true);
                    LOG.inc("txIndex>atomized>getRange>calls");
                    LOG.inc("txIndex>atomized>getRange>atoms", atoms[0]);
                    return true;
                });
            result = tx.tx(container.getBitmap(), null, -1, stackBuffer);
        } else {
            @SuppressWarnings("unchecked")
            R[] resultHolder = (R[]) new Object[1];
            bitmapIndex.get(
                (keyStream) -> keyStream.key(0, bitmapKeyBytes, 0, bitmapKeyBytes.length),
                (index, key, timestamp, tombstoned, version, payload) -> {
                    try {
                        if (payload != null) {
                            bytes.add(payload.length);
                            if (payload.length < LAST_ID_LENGTH + 4) {
                                resultHolder[0] = tx.tx(null, null, -1, null);
                                return false;
                            } else {
                                resultHolder[0] = tx.tx(null, new ByteBufferBackedFiler(payload.asByteBuffer()), LAST_ID_LENGTH, stackBuffer);
                                return false;
                            }
                        } else {
                            resultHolder[0] = tx.tx(null, null, -1, null);
                            return false;
                        }
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                },
                true);
            result = resultHolder[0];
        }

        LOG.inc("count>txIndex>total");
        LOG.inc("count>txIndex>" + name + ">total");
        LOG.inc("count>txIndex>" + name + ">" + fieldId);
        LOG.inc("bytes>txIndex>total", bytes.longValue());
        LOG.inc("bytes>txIndex>" + name + ">total", bytes.longValue());
        LOG.inc("bytes>txIndex>" + name + ">" + fieldId, bytes.longValue());
        return result;
    }

    public static <BM extends IBM, IBM> void deserNonAtomized(MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        ReusableByteBufferDataInput in,
        ByteBuffer byteBuffer,
        BitmapAndLastId<BM> container) throws IOException {

        container.clear();
        if (byteBuffer.capacity() > LAST_ID_LENGTH + 4) {
            int lastId = byteBuffer.getInt();
            byteBuffer.position(LAST_ID_LENGTH);
            in.setBuffer(byteBuffer);
            try {
                container.set(bitmaps.deserialize(in), lastId);
            } catch (Exception e) {
                trackError.error("Failed to deserialize a bitmap, length=" + byteBuffer.capacity());
                throw new IOException("Failed to deserialize", e);
            }
        }
    }

    public static <BM extends IBM, IBM> int deserLastId(MiruBitmaps<BM, IBM> bitmaps,
        boolean atomized,
        int key,
        ReusableByteBufferDataInput in,
        ByteBuffer byteBuffer) throws IOException {

        byteBuffer.clear();
        if (atomized) {
            in.setBuffer(byteBuffer);
            return bitmaps.lastIdAtomized(in, key);
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
        BitmapAndLastId<BM> index = new BitmapAndLastId<>();
        getIndexInternal(keys, index, bytes);
        BM bitmap = index.isSet() ? index.getBitmap() : bitmaps.create();
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
        bitmaps.optimize(index, keys);
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
                    ReusableByteBufferDataInput in = new ReusableByteBufferDataInput();
                    byte[] from = bitmapKeyBytes;
                    byte[] to = LABUtils.prefixUpperExclusive(bitmapKeyBytes);
                    bitmapIndex.rangeScan(from, to,
                        (index, key, timestamp, tombstoned, version, payload) -> {
                            if (payload != null) {
                                if (id[0] == -1) {
                                    bytes.add(payload.length);
                                    int labKey = LabInvertedIndex.deatomize(key.asByteBuffer());
                                    id[0] = LabInvertedIndex.deserLastId(bitmaps, atomized, labKey, in, payload.asByteBuffer());
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
            int[] delta = bitmaps.keysNotEqual(r, index);
            setIndex(delta, r);
        }
    }

    @Override
    public void or(IBM mask, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            int[] keys = bitmaps.keys(mask);
            BM index = getOrCreateIndex(keys);
            BM r = bitmaps.or(Arrays.asList(index, mask));
            int[] delta = bitmaps.keysNotEqual(r, index);
            setIndex(delta, r);
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
            int[] delta = bitmaps.keysNotEqual(andNot, index);
            setIndex(delta, andNot);
        }
    }

    @Override
    public void orToSourceSize(IBM mask, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            int[] keys = bitmaps.keys(mask);
            BM index = getOrCreateIndex(keys);
            BM or = bitmaps.orToSourceSize(index, mask);
            int[] delta = bitmaps.keysNotEqual(or, index);
            setIndex(delta, or);
        }
    }

}
