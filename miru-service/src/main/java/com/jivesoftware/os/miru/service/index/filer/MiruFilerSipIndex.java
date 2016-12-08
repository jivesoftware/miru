package com.jivesoftware.os.miru.service.index.filer;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndex;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndexMarshaller;
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class MiruFilerSipIndex<S extends MiruSipCursor<S>> implements MiruSipIndex<S> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruFilerProvider<Long, Void> sipFilerProvider;
    private final MiruSipIndexMarshaller<S> marshaller;

    private final AtomicReference<S> sipReference = new AtomicReference<>();
    private final AtomicBoolean absent = new AtomicBoolean(false);

    public MiruFilerSipIndex(MiruFilerProvider<Long, Void> sipFilerProvider, MiruSipIndexMarshaller<S> marshaller) {
        this.sipFilerProvider = sipFilerProvider;
        this.marshaller = marshaller;
    }

    @Override
    public Optional<S> getSip(StackBuffer stackBuffer) throws IOException, InterruptedException {
        S sip = sipReference.get();
        if (sip == null && !absent.get()) {
            sipFilerProvider.read(null, (monkey, filer, _stackBuffer, lock) -> {
                if (filer != null) {
                    synchronized (lock) {
                        filer.seek(0);
                        try {
                            sipReference.set(marshaller.fromFiler(filer, _stackBuffer));
                        } catch (Exception e) {
                            LOG.warn("Failed to deserialize sip, length={}", filer.getSize());
                            sipReference.set(null);
                            absent.set(true);
                        }
                    }
                } else {
                    sipReference.set(null);
                    absent.set(true);
                }
                return null;
            }, stackBuffer);
            sip = sipReference.get();
        }
        return Optional.fromNullable(sip);
    }

    @Override
    public boolean setSip(final S sip, StackBuffer stackBuffer) throws IOException, InterruptedException {
        return sipFilerProvider.readWriteAutoGrow(marshaller.expectedCapacity(sip), (monkey, filer, _stackBuffer, lock) -> {
            S existingSip = getSip(_stackBuffer).orNull();
            while (existingSip == null || sip.compareTo(existingSip) > 0) {
                if (sipReference.compareAndSet(existingSip, sip)) {
                    synchronized (lock) {
                        filer.seek(0);
                        try {
                            marshaller.toFiler(filer, sip, _stackBuffer);
                        } catch (Exception e) {
                            throw new IOException("Failed to serialize sip");
                        }
                    }
                    return true;
                } else {
                    existingSip = sipReference.get();
                }
            }
            return false;
        }, stackBuffer);
    }

    @Override
    public int getRealtimeDeliveryId(StackBuffer stackBuffer) throws Exception {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean setRealtimeDeliveryId(int deliveryId, StackBuffer stackBuffer) throws Exception {
        return true;
    }

    @Override
    public <C extends Comparable<C>> C getCustom(byte[] key, CustomMarshaller<C> deserializer) throws Exception {
        return null;
    }

    @Override
    public <C extends Comparable<C>> boolean setCustom(byte[] key, C comparable, CustomMarshaller<C> marshaller) throws Exception {
        return false;
    }

    @Override
    public void merge() throws Exception {
        LOG.warn("Unimplemented merge");
    }
}
