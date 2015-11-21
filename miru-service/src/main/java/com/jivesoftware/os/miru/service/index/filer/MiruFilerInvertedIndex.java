package com.jivesoftware.os.miru.service.index.filer;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.ChunkTransaction;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.BitmapAndLastId;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * @author jonathan
 */
public class MiruFilerInvertedIndex<BM extends IBM, IBM> implements MiruInvertedIndex<IBM> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private static final int LAST_ID_LENGTH = 4;

    private final MiruBitmaps<BM, IBM> bitmaps;
    private final MiruFieldIndex.IndexKey indexKey;
    private final KeyedFilerStore<Long, Void> keyedFilerStore;
    private final int considerIfIndexIdGreaterThanN;
    private final Object mutationLock;
    private volatile int lastId = Integer.MIN_VALUE;

    public MiruFilerInvertedIndex(MiruBitmaps<BM, IBM> bitmaps,
        MiruFieldIndex.IndexKey indexKey,
        KeyedFilerStore<Long, Void> keyedFilerStore,
        int considerIfIndexIdGreaterThanN,
        Object mutationLock) {
        this.bitmaps = bitmaps;
        this.indexKey = Preconditions.checkNotNull(indexKey);
        this.keyedFilerStore = Preconditions.checkNotNull(keyedFilerStore);
        this.considerIfIndexIdGreaterThanN = considerIfIndexIdGreaterThanN;
        this.mutationLock = mutationLock;
    }

    @Override
    public Optional<IBM> getIndex(StackBuffer stackBuffer) throws Exception {
        if (lastId > Integer.MIN_VALUE && lastId <= considerIfIndexIdGreaterThanN) {
            return Optional.absent();
        }

        byte[] rawBytes = keyedFilerStore.read(indexKey.keyBytes, null, getTransaction, stackBuffer);
        if (rawBytes != null) {
            log.inc("get>total");
            log.inc("get>bytes", rawBytes.length);
        } else {
            log.inc("get>null");
        }

        BitmapAndLastId<BM> bitmapAndLastId = deser(rawBytes);
        if (bitmapAndLastId != null) {
            if (lastId == Integer.MIN_VALUE) {
                lastId = bitmapAndLastId.lastId;
            }
            return Optional.of(bitmapAndLastId.bitmap);
        } else {
            lastId = -1;
            return Optional.absent();
        }
    }

    @Override
    public <R> R txIndex(IndexTx<R, IBM> tx, StackBuffer stackBuffer) throws Exception {
        if (lastId > Integer.MIN_VALUE && lastId <= considerIfIndexIdGreaterThanN) {
            return tx.tx(null, null);
        }

        return keyedFilerStore.read(indexKey.keyBytes, null, (monkey, filer, stackBuffer1, lock) -> {
            try {
                if (filer != null) {
                    synchronized (lock) {
                        if (filer.length() < LAST_ID_LENGTH + 4) {
                            return tx.tx(null, null);
                        } else if (filer.canLeakUnsafeByteBuffer()) {
                            ByteBuffer buffer = filer.leakUnsafeByteBuffer();
                            buffer.position(LAST_ID_LENGTH);
                            return tx.tx(null, buffer);
                        } else {
                            filer.seek(LAST_ID_LENGTH);
                            byte[] bytes = new byte[(int) filer.length() - LAST_ID_LENGTH];
                            FilerIO.read(filer, bytes);
                            return tx.tx(null, ByteBuffer.wrap(bytes));
                        }
                    }
                } else {
                    return tx.tx(null, null);
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
        }, stackBuffer);
    }

    @Override
    public void replaceIndex(IBM index, int setLastId, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            setIndex(index, setLastId, stackBuffer);
            lastId = Math.max(setLastId, lastId);
        }
    }

    private BitmapAndLastId<BM> deser(byte[] bytes) throws IOException {
        //TODO just add a byte marker, this sucks
        if (bytes != null && bytes.length > LAST_ID_LENGTH + 4) {
            if (FilerIO.bytesInt(bytes, LAST_ID_LENGTH) > 0) {
                int lastId = FilerIO.bytesInt(bytes, 0);
                DataInput dataInput = ByteStreams.newDataInput(bytes, LAST_ID_LENGTH);
                try {
                    return new BitmapAndLastId<>(bitmaps.deserialize(dataInput), lastId);
                } catch (Exception e) {
                    throw new IOException("Failed to deserialize", e);
                }
            } else {
                return new BitmapAndLastId<>(bitmaps.create(), -1);
            }
        }
        return null;
    }

    private IBM getOrCreateIndex(StackBuffer stackBuffer) throws Exception {
        Optional<IBM> index = getIndex(stackBuffer);
        return index.isPresent() ? index.get() : bitmaps.create();
    }

    private static <BM extends IBM, IBM> long serializedSizeInBytes(MiruBitmaps<BM, IBM> bitmaps, IBM index) {
        return LAST_ID_LENGTH + bitmaps.serializedSizeInBytes(index);
    }

    private void setIndex(IBM index, int setLastId, StackBuffer stackBuffer) throws Exception {
        long filerSizeInBytes = serializedSizeInBytes(bitmaps, index);
        ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput((int) filerSizeInBytes);
        dataOutput.write(FilerIO.intBytes(setLastId));
        bitmaps.serialize(index, dataOutput);
        final byte[] bytes = dataOutput.toByteArray();

        keyedFilerStore.writeNewReplace(indexKey.keyBytes, filerSizeInBytes, new SetTransaction(bytes), stackBuffer);
        log.inc("set>total");
        log.inc("set>bytes", bytes.length);
    }

    @Override
    public Optional<IBM> getIndexUnsafe(StackBuffer stackBuffer) throws Exception {
        return getIndex(stackBuffer);
    }

    @Override
    public void append(StackBuffer stackBuffer, int... ids) throws Exception {
        if (ids.length == 0) {
            return;
        }
        synchronized (mutationLock) {
            IBM index = getOrCreateIndex(stackBuffer);
            BM r = bitmaps.create();
            bitmaps.append(r, index, ids);
            int appendLastId = ids[ids.length - 1];
            if (appendLastId > lastId) {
                lastId = appendLastId;
            }

            setIndex(r, lastId, stackBuffer);
        }
    }

    @Override
    public void appendAndExtend(List<Integer> ids, int extendToId, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            IBM index = getOrCreateIndex(stackBuffer);
            BM r = bitmaps.create();
            bitmaps.extend(r, index, ids, extendToId + 1);

            if (!ids.isEmpty()) {
                int appendLastId = ids.get(ids.size() - 1);
                if (appendLastId > lastId) {
                    lastId = appendLastId;
                }
            }

            setIndex(r, lastId, stackBuffer);
        }
    }

    @Override
    public void remove(int id, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            IBM index = getOrCreateIndex(stackBuffer);
            BM r = bitmaps.create();
            bitmaps.remove(r, index, id);
            setIndex(r, lastId, stackBuffer);
        }
    }

    @Override
    public void set(StackBuffer stackBuffer, int... ids) throws Exception {
        if (ids.length == 0) {
            return;
        }
        synchronized (mutationLock) {
            IBM index = getOrCreateIndex(stackBuffer);
            BM r = bitmaps.create();
            bitmaps.set(r, index, ids);

            for (int id : ids) {
                if (id > lastId) {
                    lastId = id;
                }
            }

            setIndex(r, lastId, stackBuffer);
        }
    }

    @Override
    public int lastId(StackBuffer stackBuffer) throws Exception {
        if (lastId == Integer.MIN_VALUE) {
            synchronized (mutationLock) {
                lastId = keyedFilerStore.read(indexKey.keyBytes, null, lastIdTransaction, stackBuffer);
            }
            log.inc("lastId>total");
            log.inc("lastId>bytes", 4);

        }
        return lastId;
    }

    private static int getLastId(Object lock, Filer filer, StackBuffer stackBuffer) throws IOException {
        synchronized (lock) {
            filer.seek(0);
            return FilerIO.readInt(filer, "lastId", stackBuffer);
        }
    }

    @Override
    public void andNot(IBM mask, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            IBM index = getOrCreateIndex(stackBuffer);
            BM r = bitmaps.create();
            bitmaps.andNot(r, index, mask);
            setIndex(r, lastId, stackBuffer);
        }
    }

    @Override
    public void or(IBM mask, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            IBM index = getOrCreateIndex(stackBuffer);
            BM r = bitmaps.create();
            bitmaps.or(r, Arrays.asList(index, mask));
            setIndex(r, Math.max(lastId, bitmaps.lastSetBit(mask)), stackBuffer);
        }
    }

    @Override
    public void andNotToSourceSize(List<IBM> masks, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            IBM index = getOrCreateIndex(stackBuffer);
            BM andNot = bitmaps.create();
            bitmaps.andNotToSourceSize(andNot, index, masks);
            setIndex(andNot, lastId, stackBuffer);
        }
    }

    @Override
    public void orToSourceSize(IBM mask, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            IBM index = getOrCreateIndex(stackBuffer);
            BM or = bitmaps.create();
            bitmaps.orToSourceSize(or, index, mask);
            setIndex(or, lastId, stackBuffer);
        }
    }

    private static final ChunkTransaction<Void, Integer> lastIdTransaction = (monkey, filer, stackBuffer, lock) -> {
        if (filer != null) {
            return getLastId(lock, filer, stackBuffer);
        } else {
            return -1;
        }
    };

    private static final ChunkTransaction<Void, byte[]> getTransaction = (monkey, filer, stackBuffer, lock) -> {
        if (filer != null) {
            synchronized (lock) {
                filer.seek(0);
                byte[] bytes = new byte[(int) filer.length()];
                FilerIO.read(filer, bytes);
                return bytes;
            }
        }
        return null;
    };

    private static class SetTransaction implements ChunkTransaction<Void, Void> {

        private final byte[] bytes;

        private SetTransaction(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public Void commit(Void monkey, ChunkFiler newFiler, StackBuffer stackBuffer, Object newLock) throws IOException {
            synchronized (newLock) {
                newFiler.seek(0);
                newFiler.write(bytes);
                return null;
            }
        }
    }
}
