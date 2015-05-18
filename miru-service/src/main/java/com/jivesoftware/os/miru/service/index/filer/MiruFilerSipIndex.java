package com.jivesoftware.os.miru.service.index.filer;

import com.google.common.base.Optional;
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
    public Optional<S> getSip() throws IOException {
        S sip = sipReference.get();
        if (sip == null && !absent.get()) {
            sipFilerProvider.read(null, (monkey, filer, lock) -> {
                if (filer != null) {
                    synchronized (lock) {
                        filer.seek(0);
                        try {
                            sipReference.set(marshaller.fromFiler(filer));
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
            });
            sip = sipReference.get();
        }
        return Optional.fromNullable(sip);
    }

    @Override
    public boolean setSip(final S sip) throws IOException {
        return sipFilerProvider.readWriteAutoGrow(marshaller.expectedCapacity(sip), (monkey, filer, lock) -> {
            S existingSip = getSip().orNull();
            while (existingSip == null || sip.compareTo(existingSip) > 0) {
                if (sipReference.compareAndSet(existingSip, sip)) {
                    synchronized (lock) {
                        filer.seek(0);
                        try {
                            marshaller.toFiler(filer, sip);
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
        });
    }
}
