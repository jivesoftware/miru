package com.jivesoftware.os.miru.service.index.lab;

import com.google.common.base.Optional;
import com.google.common.primitives.Bytes;
import com.jivesoftware.os.filer.io.ByteArrayFiler;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.collections.bah.ConcurrentBAHash;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndex;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndexMarshaller;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class LabSipIndex<S extends MiruSipCursor<S>> implements MiruSipIndex<S> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider idProvider;
    private final ValueIndex<byte[]> valueIndex;
    private final byte[] sipKey;
    private final byte[] realtimeDeliveryIdKey;
    private final byte[] customPrefix;
    private final MiruSipIndexMarshaller<S> marshaller;

    private final AtomicReference<S> sipReference = new AtomicReference<>();
    private final AtomicBoolean sipAbsent = new AtomicBoolean(false);

    private final AtomicReference<ConcurrentBAHash<byte[]>> customStage = new AtomicReference<>();
    private final AtomicReference<ConcurrentBAHash<byte[]>> customMerge = new AtomicReference<>();

    public LabSipIndex(OrderIdProvider idProvider,
        ValueIndex<byte[]> valueIndex,
        byte[] sipKey,
        byte[] realtimeDeliveryIdKey,
        byte[] customPrefix,
        MiruSipIndexMarshaller<S> marshaller) {
        this.idProvider = idProvider;
        this.valueIndex = valueIndex;
        this.sipKey = sipKey;
        this.realtimeDeliveryIdKey = realtimeDeliveryIdKey;
        this.customPrefix = customPrefix;
        this.marshaller = marshaller;
    }

    @Override
    public Optional<S> getSip(StackBuffer stackBuffer) throws Exception {
        S sip = sipReference.get();
        if (sip == null && !sipAbsent.get()) {
            valueIndex.get(
                (keyStream) -> keyStream.key(0, sipKey, 0, sipKey.length),
                (index, key1, timestamp, tombstoned, version, payload) -> {
                    if (payload != null && !tombstoned) {
                        try {
                            sipReference.set(marshaller.fromFiler(new ByteBufferBackedFiler(payload.asByteBuffer()), stackBuffer));
                        } catch (Exception e) {
                            LOG.warn("Failed to deserialize sip, length={}", payload.length);
                            sipReference.set(null);
                            sipAbsent.set(true);
                        }
                    } else {
                        sipReference.set(null);
                        sipAbsent.set(true);
                    }
                    return true;
                },
                true);

            sip = sipReference.get();
        }
        return Optional.fromNullable(sip);
    }

    @Override
    public boolean setSip(final S sip, StackBuffer stackBuffer) throws Exception {
        S existingSip = getSip(stackBuffer).orNull();
        while (existingSip == null || sip.compareTo(existingSip) > 0) {
            if (sipReference.compareAndSet(existingSip, sip)) {
                return true;
            } else {
                existingSip = sipReference.get();
            }
        }
        return false;
    }

    @Override
    public int getRealtimeDeliveryId(StackBuffer stackBuffer) throws Exception {
        int[] deliveryId = { -1 };
        valueIndex.get(
            (keyStream) -> keyStream.key(0, realtimeDeliveryIdKey, 0, realtimeDeliveryIdKey.length),
            (index, key1, timestamp, tombstoned, version, payload) -> {
                if (payload != null && !tombstoned) {
                    deliveryId[0] = payload.getInt(0);
                }
                return true;
            },
            true);
        return deliveryId[0];
    }

    @Override
    public boolean setRealtimeDeliveryId(int deliveryId, StackBuffer stackBuffer) throws Exception {
        return valueIndex.append(stream -> {
            return stream.stream(-1, realtimeDeliveryIdKey, System.currentTimeMillis(), false, idProvider.nextId(), FilerIO.intBytes(deliveryId));
        }, true, new BolBuffer(), new BolBuffer());
    }

    @Override
    public <C extends Comparable<C>> C getCustom(byte[] key, CustomMarshaller<C> marshaller) throws Exception {
        ConcurrentBAHash<byte[]> custom = customStage.get();
        byte[] got = (custom == null) ? null : custom.get(key);
        if (got == null) {
            custom = customMerge.get();
            got = (custom == null) ? null : custom.get(key);
        }
        if (got == null) {
            byte[] customKey = customKey(key);
            byte[][] result = new byte[1][];
            valueIndex.get(
                (keyStream) -> keyStream.key(0, customKey, 0, customKey.length),
                (index, key1, timestamp, tombstoned, version, payload) -> {
                    if (payload != null && !tombstoned) {
                        try {
                            result[0] = payload.copy();
                        } catch (Exception e) {
                            LOG.warn("Failed to deserialize custom, length={}", payload.length);
                        }
                    }
                    return true;
                },
                true);

            got = result[0];
        }
        return (got == null) ? null : marshaller.fromBytes(got);
    }

    @Override
    public <C extends Comparable<C>> boolean setCustom(byte[] key, C comparable, CustomMarshaller<C> marshaller) throws Exception {
        if (comparable == null) {
            return false;
        }
        synchronized (customStage) {
            boolean[] result = { false };
            ConcurrentBAHash<byte[]> got = customStage.updateAndGet(existing -> {
                return existing != null ? existing : new ConcurrentBAHash<>(13, true, 4);
            });
            got.compute(key, (_key, value) -> {
                C existing = (value == null) ? null : marshaller.fromBytes(value);
                if (existing == null || comparable.compareTo(existing) > 0) {
                    result[0] = true;
                    return marshaller.toBytes(comparable);
                } else {
                    return value;
                }
            });
            return result[0];
        }
    }

    private byte[] customKey(byte[] key) {
        return Bytes.concat(customPrefix, key);
    }

    @Override
    public void merge() throws Exception {
        if (customMerge.get() == null) {
            synchronized (customStage) {
                ConcurrentBAHash<byte[]> got = customStage.getAndSet(null);
                customMerge.set(got);
            }
        }
        valueIndex.append(
            stream -> {
                long timestamp = System.currentTimeMillis();
                long version = idProvider.nextId();

                S sip = sipReference.get();
                if (sip != null) {
                    ByteArrayFiler filer = new ByteArrayFiler();
                    marshaller.toFiler(filer, sip, new StackBuffer());
                    LOG.inc("sip>merge>primary");
                    if (!stream.stream(-1, sipKey, timestamp, false, version, filer.getBytes())) {
                        return false;
                    }
                }

                ConcurrentBAHash<byte[]> custom = customMerge.get();
                if (custom != null) {
                    boolean result = custom.stream((key, value) -> {
                        LOG.inc("sip>merge>custom");
                        return stream.stream(-1, customKey(key), timestamp, false, version, value);
                    });
                    if (result) {
                        customMerge.set(null);
                    } else {
                        return false;
                    }
                }

                return true;
            },
            true,
            new BolBuffer(),
            new BolBuffer());
    }

}
