package com.jivesoftware.os.miru.service.index.filer;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.BitmapAndLastId;
import com.jivesoftware.os.miru.service.stream.KeyedIndex;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author jonathan
 */
public class KeyedIndexInvertedIndex<BM extends IBM, IBM> implements MiruInvertedIndex<IBM> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private static final int LAST_ID_LENGTH = 4;

    private final MiruBitmaps<BM, IBM> bitmaps;
    private final MiruFieldIndex.IndexKey indexKey;
    private final KeyedIndex keyedIndex;
    private final int considerIfIndexIdGreaterThanN;
    private final Object mutationLock;
    private volatile int lastId = Integer.MIN_VALUE;

    public KeyedIndexInvertedIndex(MiruBitmaps<BM, IBM> bitmaps,
        MiruFieldIndex.IndexKey indexKey,
        KeyedIndex keyedIndex,
        int considerIfIndexIdGreaterThanN,
        Object mutationLock) {
        this.bitmaps = bitmaps;
        this.indexKey = Preconditions.checkNotNull(indexKey);
        this.keyedIndex = Preconditions.checkNotNull(keyedIndex);
        this.considerIfIndexIdGreaterThanN = considerIfIndexIdGreaterThanN;
        this.mutationLock = mutationLock;
    }

    @Override
    public Optional<IBM> getIndex(StackBuffer stackBuffer) throws Exception {
        if (lastId > Integer.MIN_VALUE && lastId <= considerIfIndexIdGreaterThanN) {
            return Optional.absent();
        }

        byte[] rawBytes = keyedIndex.get(indexKey.keyBytes);
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
        return keyedIndex.tx(indexKey.keyBytes, (value, buffer) -> {
            if (value != null) {
                buffer = ByteBuffer.wrap(value);
                buffer.position(LAST_ID_LENGTH);
                return tx.tx(null, buffer, null, stackBuffer);
            } else if (buffer != null) {
                buffer.position(LAST_ID_LENGTH);
                return tx.tx(null, buffer, null, stackBuffer);
            } else {
                return tx.tx(null, null, null, stackBuffer);
            }
        });
    }

    @Override
    public void replaceIndex(IBM index, int setLastId, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            setIndex(index, setLastId);
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

    private void setIndex(IBM index, int setLastId) throws Exception {
        long filerSizeInBytes = serializedSizeInBytes(bitmaps, index);
        ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput((int) filerSizeInBytes);
        dataOutput.writeInt(setLastId);
        bitmaps.serialize(index, dataOutput);
        final byte[] bytes = dataOutput.toByteArray();

        keyedIndex.put(indexKey.keyBytes, bytes);
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

            setIndex(r, lastId);
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

            setIndex(r, lastId);
        }
    }

    @Override
    public void remove(int id, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            IBM index = getOrCreateIndex(stackBuffer);
            BM r = bitmaps.create();
            bitmaps.remove(r, index, id);
            setIndex(r, lastId);
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

            setIndex(r, lastId);
        }
    }

    @Override
    public int lastId(StackBuffer stackBuffer) throws Exception {
        if (lastId == Integer.MIN_VALUE) {
            synchronized (mutationLock) {
                lastId = keyedIndex.tx(indexKey.keyBytes, (byte[] value, ByteBuffer buffer) -> {
                    if (value != null) {
                        return FilerIO.bytesInt(value, 0);
                    } else if (buffer != null) {
                        return buffer.getInt();
                    } else {
                        return -1;
                    }
                });
            }
            log.inc("lastId>total");
            log.inc("lastId>bytes", 4);

        }
        return lastId;
    }

    @Override
    public void andNot(IBM mask, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            IBM index = getOrCreateIndex(stackBuffer);
            BM r = bitmaps.create();
            bitmaps.andNot(r, index, Collections.singletonList(mask));
            setIndex(r, lastId);
        }
    }

    @Override
    public void or(IBM mask, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            IBM index = getOrCreateIndex(stackBuffer);
            BM r = bitmaps.create();
            bitmaps.or(r, Arrays.asList(index, mask));
            setIndex(r, Math.max(lastId, bitmaps.lastSetBit(mask)));
        }
    }

    @Override
    public void andNotToSourceSize(List<IBM> masks, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            IBM index = getOrCreateIndex(stackBuffer);
            BM andNot = bitmaps.create();
            bitmaps.andNotToSourceSize(andNot, index, masks);
            setIndex(andNot, lastId);
        }
    }

    @Override
    public void orToSourceSize(IBM mask, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            IBM index = getOrCreateIndex(stackBuffer);
            BM or = bitmaps.create();
            bitmaps.orToSourceSize(or, index, mask);
            setIndex(or, lastId);
        }
    }
}
