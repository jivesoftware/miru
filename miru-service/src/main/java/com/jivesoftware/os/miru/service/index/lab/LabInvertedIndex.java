package com.jivesoftware.os.miru.service.index.lab;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.jivesoftware.os.filer.io.ByteArrayFiler;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.io.api.UIO;
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
public class LabInvertedIndex<BM extends IBM, IBM> implements MiruInvertedIndex<BM, IBM> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static final int LAST_ID_LENGTH = 4;

    private final OrderIdProvider idProvider;
    private final MiruBitmaps<BM, IBM> bitmaps;
    private final TrackError trackError;
    private final String name;
    private final int fieldId;
    private final byte[] indexKeyBytes;
    private final ValueIndex bitmapIndex;
    private final ValueIndex termIndex;
    private final Object mutationLock;
    private volatile int lastId = Integer.MIN_VALUE;

    public LabInvertedIndex(OrderIdProvider idProvider,
        MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        String name,
        int fieldId,
        byte[] indexKeyBytes,
        ValueIndex bitmapIndex,
        ValueIndex termIndex,
        Object mutationLock) {

        this.idProvider = idProvider;
        this.bitmaps = bitmaps;
        this.trackError = trackError;
        this.name = name;
        this.fieldId = fieldId;
        this.indexKeyBytes = Preconditions.checkNotNull(indexKeyBytes);
        this.bitmapIndex = Preconditions.checkNotNull(bitmapIndex);
        this.termIndex = termIndex;
        this.mutationLock = mutationLock;
    }

    @Override
    public Optional<BM> getIndex(StackBuffer stackBuffer) throws Exception {
        MutableLong bytes = new MutableLong();
        Optional<BM> index = getIndexInternal(bytes, -1, stackBuffer).transform(input -> input.bitmap);
        LOG.inc("count>getIndex>total");
        LOG.inc("count>getIndex>" + name + ">total");
        LOG.inc("count>getIndex>" + name + ">" + fieldId);
        LOG.inc("bytes>getIndex>total", bytes.longValue());
        LOG.inc("bytes>getIndex>" + name + ">total", bytes.longValue());
        LOG.inc("bytes>getIndex>" + name + ">" + fieldId, bytes.longValue());
        return index;
    }

    @Override
    public Optional<BitmapAndLastId<BM>> getIndexAndLastId(int considerIfLastIdGreaterThanN, StackBuffer stackBuffer) throws Exception {
        MutableLong bytes = new MutableLong();
        Optional<BitmapAndLastId<BM>> index = getIndexInternal(bytes, considerIfLastIdGreaterThanN, stackBuffer);
        LOG.inc("count>getIndexAndLastId>total");
        LOG.inc("count>getIndexAndLastId>" + name + ">total");
        LOG.inc("count>getIndexAndLastId>" + name + ">" + fieldId);
        LOG.inc("bytes>getIndexAndLastId>total", bytes.longValue());
        LOG.inc("bytes>getIndexAndLastId>" + name + ">total", bytes.longValue());
        LOG.inc("bytes>getIndexAndLastId>" + name + ">" + fieldId, bytes.longValue());
        return index;
    }

    private Optional<BitmapAndLastId<BM>> getIndexInternal(MutableLong bytes, int considerIfLastIdGreaterThanN, StackBuffer stackBuffer) throws Exception {

        @SuppressWarnings("unchecked")
        BitmapAndLastId<BM>[] bitmapAndLastId = new BitmapAndLastId[1];
        bitmapIndex.get((keyStream) -> keyStream.key(0, indexKeyBytes, 0, indexKeyBytes.length),
            (int index, byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) -> {
                if (payload != null) {
                    bitmapAndLastId[0] = deser(bitmaps, trackError, payload, considerIfLastIdGreaterThanN);
                    bytes.add(payload.length);
                }
                return true;
            }, true);

        if (bitmapAndLastId[0] != null) {
            LOG.inc("get>hit");
            if (lastId == Integer.MIN_VALUE) {
                lastId = bitmapAndLastId[0].lastId;
            }
            return Optional.of(bitmapAndLastId[0]);
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
            (keyStream) -> keyStream.key(0, indexKeyBytes, 0, indexKeyBytes.length),
            (int index, byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) -> {
                try {
                    if (payload != null) {
                        bytes.add(payload.length);
                        if (payload.length < LAST_ID_LENGTH + 4) {
                            result[0] = tx.tx(null, null, -1, null);
                            return false;
                        } else {
                            result[0] = tx.tx(null, new ByteArrayFiler(payload), LAST_ID_LENGTH, stackBuffer);
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

    @Override
    public void replaceIndex(IBM index, int setLastId, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            setIndex(index, setLastId);
            lastId = Math.max(setLastId, lastId);
        }
    }

    public static <BM extends IBM, IBM> BitmapAndLastId<BM> deser(MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        byte[] bytes,
        int considerIfLastIdGreaterThanN) throws IOException {

        if (bytes.length > LAST_ID_LENGTH + 4) {
            int lastId = UIO.bytesInt(bytes, 0);
            if (considerIfLastIdGreaterThanN < 0 || lastId > considerIfLastIdGreaterThanN) {
                DataInput dataInput = ByteStreams.newDataInput(bytes, LAST_ID_LENGTH);
                try {
                    return new BitmapAndLastId<>(bitmaps.deserialize(dataInput), lastId);
                } catch (Exception e) {
                    try {
                        trackError.error("Failed to deserialize a bitmap, length=" + bytes.length);
                        LOG.error("Failed to deserialize, head bytes");
                    } catch (Exception e1) {
                        LOG.error("Failed to print debug info", e1);
                    }
                    throw new IOException("Failed to deserialize", e);
                }
            }
        }
        return null;
    }

    private BM getOrCreateIndex(StackBuffer stackBuffer) throws Exception {
        Optional<BM> index = getIndex(stackBuffer);
        return index.isPresent() ? index.get() : bitmaps.create();
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

    private void setIndex(IBM index, int setLastId) throws Exception {
        SizeAndBytes sizeAndBytes = getSizeAndBytes(bitmaps, index, setLastId);

        long timestamp = System.currentTimeMillis();
        long version = idProvider.nextId();
        if (termIndex != null) {
            termIndex.append(stream -> {
                if (!stream.stream(-1, indexKeyBytes, timestamp, false, version, null)) {
                    return false;
                }
                return true;
            }, true);
        }
        bitmapIndex.append(stream -> {
            if (!stream.stream(-1, indexKeyBytes, timestamp, false, version, sizeAndBytes.bytes)) {
                return false;
            }
            return true;
        }, true);

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
            setIndex(r, lastId);
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

            setIndex(r, lastId);
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
                int[] id = {-1};
                bitmapIndex.get(
                    (keyStream) -> keyStream.key(0, indexKeyBytes, 0, indexKeyBytes.length),
                    (int index, byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) -> {
                        if (payload != null) {
                            bytes.add(payload.length);
                            id[0] = UIO.bytesInt(payload);
                        }
                        return true;
                    },
                    true
                );
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
            BM index = getOrCreateIndex(stackBuffer);
            BM r = bitmaps.andNot(index, mask);
            setIndex(r, lastId);
        }
    }

    @Override
    public void or(IBM mask, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            BM index = getOrCreateIndex(stackBuffer);
            BM r = bitmaps.or(Arrays.asList(index, mask));
            setIndex(r, Math.max(lastId, bitmaps.lastSetBit(mask)));
        }
    }

    @Override
    public void andNotToSourceSize(List<IBM> masks, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            BM index = getOrCreateIndex(stackBuffer);
            BM andNot = bitmaps.andNotToSourceSize(index, masks);
            setIndex(andNot, lastId);
        }
    }

    @Override
    public void orToSourceSize(IBM mask, StackBuffer stackBuffer) throws Exception {
        synchronized (mutationLock) {
            BM index = getOrCreateIndex(stackBuffer);
            BM or = bitmaps.orToSourceSize(index, mask);
            setIndex(or, lastId);
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

}
