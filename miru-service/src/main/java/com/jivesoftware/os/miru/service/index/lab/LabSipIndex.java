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
    private final ValueIndex valueIndex;
    private final byte[] sipKey;
    private final byte[] realtimeDeliveryIdKey;
    private final byte[] customPrefix;
    private final ConcurrentBAHash<byte[]> customSip;
    private final ConcurrentBAHash<Boolean> customAbsent;
    private final MiruSipIndexMarshaller<S> marshaller;

    private final AtomicReference<S> sipReference = new AtomicReference<>();
    private final AtomicBoolean sipAbsent = new AtomicBoolean(false);

    public LabSipIndex(OrderIdProvider idProvider,
        ValueIndex valueIndex,
        byte[] sipKey,
        byte[] realtimeDeliveryIdKey,
        byte[] customPrefix,
        ConcurrentBAHash<byte[]> customSip,
        ConcurrentBAHash<Boolean> customAbsent,
        MiruSipIndexMarshaller<S> marshaller) {
        this.idProvider = idProvider;
        this.valueIndex = valueIndex;
        this.sipKey = sipKey;
        this.realtimeDeliveryIdKey = realtimeDeliveryIdKey;
        this.customPrefix = customPrefix;
        this.customSip = customSip;
        this.customAbsent = customAbsent;
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
                true
            );

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
            true
        );
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
        byte[] got = customSip.get(key);
        if (got == null && customAbsent.get(key) == null) {
            byte[] customKey = Bytes.concat(customPrefix, key);
            valueIndex.get(
                (keyStream) -> keyStream.key(0, customKey, 0, customKey.length),
                (index, key1, timestamp, tombstoned, version, payload) -> {
                    if (payload != null && !tombstoned) {
                        try {
                            byte[] bytes = payload.copy();
                            customSip.put(key, bytes);
                        } catch (Exception e) {
                            LOG.warn("Failed to deserialize custom, length={}", payload.length);
                            customSip.remove(key);
                            customAbsent.put(key, true);
                        }
                    } else {
                        customSip.remove(key);
                        customAbsent.put(key, true);
                    }
                    return true;
                },
                true
            );

            got = customSip.get(key);
        }
        return (got == null) ? null : marshaller.fromBytes(got);
    }

    @Override
    public <C extends Comparable<C>> boolean setCustom(byte[] key, C comparable, CustomMarshaller<C> marshaller) throws Exception {
        if (comparable == null) {
            return false;
        }
        boolean[] result = { false };
        customSip.compute(key, (_key, value) -> {
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

    @Override
    public void merge() throws Exception {
        valueIndex.append(
            stream -> {
                S sip = sipReference.get();
                if (sip != null) {
                    ByteArrayFiler filer = new ByteArrayFiler();
                    marshaller.toFiler(filer, sip, new StackBuffer());
                    if (!stream.stream(-1, sipKey, System.currentTimeMillis(), false, idProvider.nextId(), filer.getBytes())) {
                        return false;
                    }
                }

                return customSip.stream((key, value) -> {
                    return stream.stream(-1, Bytes.concat(customPrefix, key), System.currentTimeMillis(), false, idProvider.nextId(), value);
                });
            },
            true,
            new BolBuffer(),
            new BolBuffer());
    }

}
