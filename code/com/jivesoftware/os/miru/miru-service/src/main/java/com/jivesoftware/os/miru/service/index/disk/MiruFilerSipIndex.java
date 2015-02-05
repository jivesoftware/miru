package com.jivesoftware.os.miru.service.index.disk;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndex;
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class MiruFilerSipIndex implements MiruSipIndex {

    private final MiruFilerProvider sipFilerProvider;
    private final AtomicLong sipTimestamp = new AtomicLong(-1);

    public MiruFilerSipIndex(MiruFilerProvider sipFilerProvider) {
        this.sipFilerProvider = sipFilerProvider;
    }

    @Override
    public long getSip() throws IOException {
        long sipTime = sipTimestamp.get();
        if (sipTime < 0) {
            sipFilerProvider.read(-1, new FilerTransaction<Filer, Void>() {
                @Override
                public Void commit(Object lock, Filer filer) throws IOException {
                    if (filer != null) {
                        synchronized (lock) {
                            filer.seek(0);
                            sipTimestamp.set(FilerIO.readLong(filer, "timestamp"));
                        }
                    } else {
                        sipTimestamp.set(0);
                    }
                    return null;
                }
            });
            sipTime = sipTimestamp.get();
        }
        return sipTime;
    }

    @Override
    public boolean setSip(final long timestamp) throws IOException {
        return sipFilerProvider.readWriteAutoGrow(8, new FilerTransaction<Filer, Boolean>() {
            @Override
            public Boolean commit(Object lock, Filer filer) throws IOException {
                long existingTime = sipTimestamp.get();
                while (timestamp > existingTime) {
                    if (sipTimestamp.compareAndSet(existingTime, timestamp)) {
                        synchronized (lock) {
                            filer.seek(0);
                            FilerIO.writeLong(filer, timestamp, "timestamp");
                        }
                        return true;
                    } else {
                        existingTime = sipTimestamp.get();
                    }
                }
                return false;
            }
        });
    }
}
