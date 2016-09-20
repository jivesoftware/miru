package com.jivesoftware.os.miru.service.index.filer;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerDataInput;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.ChunkTransaction;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.IndexTx;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * @author jonathan
 */
public class MiruFilerInvertedIndex<BM extends IBM, IBM> implements MiruInvertedIndex<BM, IBM> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static final int LAST_ID_LENGTH = 4;

    private final MiruBitmaps<BM, IBM> bitmaps;
    private final TrackError trackError;
    private final String name;
    private final int fieldId;
    private final byte[] indexKeyBytes;
    private final KeyedFilerStore<Long, Void> keyedFilerStore;
    private final Object mutationLock;
    private volatile int lastId = Integer.MIN_VALUE;

    public MiruFilerInvertedIndex(MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        String name,
        int fieldId,
        byte[] indexKeyBytes,
        KeyedFilerStore<Long, Void> keyedFilerStore,
        Object mutationLock) {
        this.bitmaps = bitmaps;
        this.trackError = trackError;
        this.name = name;
        this.fieldId = fieldId;
        this.indexKeyBytes = Preconditions.checkNotNull(indexKeyBytes);
        this.keyedFilerStore = Preconditions.checkNotNull(keyedFilerStore);
        this.mutationLock = mutationLock;
    }

    @Override
    public void getIndex(BitmapAndLastId<BM> container, StackBuffer stackBuffer) throws Exception {
        MutableLong bytes = new MutableLong();
        container.clear();
        keyedFilerStore.read(indexKeyBytes,
            null,
            (monkey, filer, stackBuffer1, lock) -> {
                if (filer != null) {
                    bytes.add(filer.length());
                    synchronized (lock) {
                        filer.seek(0);
                        deser(bitmaps, trackError, filer, -1, container, stackBuffer1);
                        return null;
                    }
                }
                return null;
            },
            stackBuffer);

        if (container.isSet()) {
            LOG.inc("get>hit");
            if (lastId == Integer.MIN_VALUE) {
                lastId = container.getLastId();
            }
        } else {
            LOG.inc("get>miss");
            lastId = -1;
        }

        LOG.inc("count>getIndex>total");
        LOG.inc("count>getIndex>" + name + ">total");
        LOG.inc("count>getIndex>" + name + ">" + fieldId);
        LOG.inc("bytes>getIndex>total", bytes.longValue());
        LOG.inc("bytes>getIndex>" + name + ">total", bytes.longValue());
        LOG.inc("bytes>getIndex>" + name + ">" + fieldId, bytes.longValue());
    }

    @Override
    public <R> R txIndex(IndexTx<R, IBM> tx, StackBuffer stackBuffer) throws Exception {
        MutableLong bytes = new MutableLong();
        R result = keyedFilerStore.read(indexKeyBytes, null, (monkey, filer, stackBuffer1, lock) -> {
            try {
                if (filer != null) {
                    bytes.add(filer.length());
                    synchronized (lock) {
                        if (filer.length() < LAST_ID_LENGTH + 4) {
                            return tx.tx(null, null, -1, null);
                        } else {
                            return tx.tx(null, filer, LAST_ID_LENGTH, stackBuffer1);
                        }
                    }
                } else {
                    return tx.tx(null, null, -1, null);
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
        }, stackBuffer);
        LOG.inc("count>txIndex>total");
        LOG.inc("count>txIndex>" + name + ">total");
        LOG.inc("count>txIndex>" + name + ">" + fieldId);
        LOG.inc("bytes>txIndex>total", bytes.longValue());
        LOG.inc("bytes>txIndex>" + name + ">total", bytes.longValue());
        LOG.inc("bytes>txIndex>" + name + ">" + fieldId, bytes.longValue());
        return result;
    }

    public static <BM extends IBM, IBM> void deser(MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        ChunkFiler filer,
        int considerIfLastIdGreaterThanN,
        BitmapAndLastId<BM> container,
        StackBuffer stackBuffer) throws IOException {

        container.clear();
        if (filer.length() > LAST_ID_LENGTH + 4) {
            int lastId = filer.readInt();
            if (considerIfLastIdGreaterThanN < 0 || lastId > considerIfLastIdGreaterThanN) {
                DataInput dataInput = new FilerDataInput(filer, stackBuffer);
                try {
                    container.set(bitmaps.deserialize(dataInput), lastId);
                } catch (Exception e) {
                    try {
                        trackError.error("Failed to deserialize a bitmap, length=" + filer.length());

                        byte[] raw = new byte[512];
                        filer.seek(0);
                        filer.read(raw);
                        LOG.error("Failed to deserialize, head bytes from filer: {}", Arrays.toString(raw));
                    } catch (Exception e1) {
                        LOG.error("Failed to print debug info", e1);
                    }
                    throw new IOException("Failed to deserialize", e);
                }
            }
        }
    }

    public static int deserLastId(ChunkFiler filer) throws IOException {
        if (filer.length() > LAST_ID_LENGTH) {
            return filer.readInt();
        }
        return -1;
    }

    private BM getOrCreateIndex(StackBuffer stackBuffer) throws Exception {
        BitmapAndLastId<BM> container = new BitmapAndLastId<>();
        getIndex(container, stackBuffer);
        return container.isSet() ? container.getBitmap() : bitmaps.create();
    }

    private static <BM extends IBM, IBM> long serializedSizeInBytes(MiruBitmaps<BM, IBM> bitmaps, IBM index) {
        return LAST_ID_LENGTH + bitmaps.serializedSizeInBytes(index);
    }

    public static <BM extends IBM, IBM> SizeAndBytes getSizeAndBytes(MiruBitmaps<BM, IBM> bitmaps, IBM index, int lastId) throws Exception {
        long filerSizeInBytes = serializedSizeInBytes(bitmaps, index);
        ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput((int) filerSizeInBytes);
        dataOutput.write(FilerIO.intBytes(lastId));
        bitmaps.serialize(index, dataOutput);
        final byte[] bytes = dataOutput.toByteArray();
        return new SizeAndBytes(filerSizeInBytes, bytes);
    }

    private void setIndex(IBM index, int setLastId, StackBuffer stackBuffer) throws Exception {
        SizeAndBytes sizeAndBytes = getSizeAndBytes(bitmaps, index, setLastId);
        keyedFilerStore.writeNewReplace(indexKeyBytes, sizeAndBytes.filerSizeInBytes, new SetTransaction(sizeAndBytes.bytes), stackBuffer);
        LOG.inc("count>set>total");
        LOG.inc("count>set>" + name + ">total");
        LOG.inc("count>set>" + name + ">" + fieldId);
        LOG.inc("bytes>set>total", sizeAndBytes.bytes.length);
        LOG.inc("bytes>set>" + name + ">total", sizeAndBytes.bytes.length);
        LOG.inc("bytes>set>" + name + ">" + fieldId, sizeAndBytes.bytes.length);
    }

    @Override
    public void remove(StackBuffer stackBuffer, int... ids) throws Exception {
        synchronized (mutationLock) {
            BM index = getOrCreateIndex(stackBuffer);
            BM r = bitmaps.remove(index, ids);
            setIndex(r, lastId, stackBuffer);
        }
    }

    @Override
    public void set(StackBuffer stackBuffer, int... ids) throws Exception {
        if (ids.length == 0) {
            return;
        }
        synchronized (mutationLock) {
            BM index = getOrCreateIndex(stackBuffer);
            BM r = bitmaps.set(index, ids);

            for (int id : ids) {
                if (id > lastId) {
                    lastId = id;
                }
            }

            setIndex(r, lastId, stackBuffer);
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
                lastId = keyedFilerStore.read(indexKeyBytes, null, (monkey, filer, stackBuffer1, lock) -> {
                    if (filer != null) {
                        bytes.add(filer.length());
                        return getLastId(lock, filer, stackBuffer1);
                    } else {
                        return -1;
                    }
                }, stackBuffer);
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

    private static int getLastId(Object lock, Filer filer, StackBuffer stackBuffer) throws IOException {
        synchronized (lock) {
            filer.seek(0);
            return FilerIO.readInt(filer, "lastId", stackBuffer);
        }
    }

    @Override
    public void andNot(IBM mask, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            BM index = getOrCreateIndex(stackBuffer);
            BM r = bitmaps.andNot(index, mask);
            setIndex(r, lastId, stackBuffer);
        }
    }

    @Override
    public void or(IBM mask, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            BM index = getOrCreateIndex(stackBuffer);
            BM r = bitmaps.or(Arrays.asList(index, mask));
            setIndex(r, Math.max(lastId, bitmaps.lastSetBit(mask)), stackBuffer);
        }
    }

    @Override
    public void andNotToSourceSize(List<IBM> masks, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            BM index = getOrCreateIndex(stackBuffer);
            BM andNot = bitmaps.andNotToSourceSize(index, masks);
            setIndex(andNot, lastId, stackBuffer);
        }
    }

    @Override
    public void orToSourceSize(IBM mask, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            BM index = getOrCreateIndex(stackBuffer);
            BM or = bitmaps.orToSourceSize(index, mask);
            setIndex(or, lastId, stackBuffer);
        }
    }

    public static class SizeAndBytes {
        public final long filerSizeInBytes;
        public final byte[] bytes;

        public SizeAndBytes(long filerSizeInBytes, byte[] bytes) {
            this.filerSizeInBytes = filerSizeInBytes;
            this.bytes = bytes;
        }
    }

    public static class SetTransaction implements ChunkTransaction<Void, Void> {

        private final byte[] bytes;

        public SetTransaction(byte[] bytes) {
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
