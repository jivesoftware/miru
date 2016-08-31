package com.jivesoftware.os.miru.service.index.lab;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.ByteArrayFiler;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.io.api.UIO;
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
    private final MiruSipIndexMarshaller<S> marshaller;

    private final AtomicReference<S> sipReference = new AtomicReference<>();
    private final AtomicBoolean absent = new AtomicBoolean(false);

    public LabSipIndex(OrderIdProvider idProvider,
        ValueIndex valueIndex,
        byte[] sipKey,
        byte[] realtimeDeliveryIdKey,
        MiruSipIndexMarshaller<S> marshaller) {
        this.idProvider = idProvider;
        this.valueIndex = valueIndex;
        this.sipKey = sipKey;
        this.realtimeDeliveryIdKey = realtimeDeliveryIdKey;
        this.marshaller = marshaller;
    }

    @Override
    public Optional<S> getSip(StackBuffer stackBuffer) throws Exception {
        S sip = sipReference.get();
        if (sip == null && !absent.get()) {
            valueIndex.get(
                (keyStream) -> keyStream.key(0, sipKey, 0, sipKey.length),
                (index, key1, timestamp, tombstoned, version, payload) -> {
                    if (payload != null && !tombstoned) {
                        try {
                            payload.clear();
                            sipReference.set(marshaller.fromFiler(new ByteBufferBackedFiler(payload), stackBuffer));
                        } catch (Exception e) {
                            LOG.warn("Failed to deserialize sip, length={}", payload.capacity());
                            sipReference.set(null);
                            absent.set(true);
                        }
                    } else {
                        sipReference.set(null);
                        absent.set(true);
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
                    payload.clear();
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
            return stream.stream(-1, sipKey, System.currentTimeMillis(), false, idProvider.nextId(), UIO.intBytes(deliveryId));
        }, true);
    }

    @Override
    public void merge() throws Exception {
        S sip = sipReference.get();
        if (sip != null) {
            ByteArrayFiler filer = new ByteArrayFiler();
            marshaller.toFiler(filer, sip, new StackBuffer());
            valueIndex.append(stream -> {
                stream.stream(-1, sipKey, System.currentTimeMillis(), false, idProvider.nextId(), filer.getBytes());
                return true;
            }, true);
        }
    }

}
