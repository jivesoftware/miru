package com.jivesoftware.os.miru.service.index.filer;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.ChunkTransaction;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.miru.api.wal.Sip;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndex;
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class MiruFilerSipIndex implements MiruSipIndex {

    private final MiruFilerProvider<Long, Void> sipFilerProvider;
    private final AtomicReference<Sip> sipReference = new AtomicReference<>();

    public MiruFilerSipIndex(MiruFilerProvider sipFilerProvider) {
        this.sipFilerProvider = sipFilerProvider;
    }

    @Override
    public Sip getSip() throws IOException {
        Sip sip = sipReference.get();
        if (sip == null) {
            sipFilerProvider.read(null, new ChunkTransaction<Void, Void>() {
                @Override
                public Void commit(Void monkey, ChunkFiler filer, Object lock) throws IOException {
                    if (filer != null) {
                        synchronized (lock) {
                            filer.seek(0);
                            long clockTimestamp = 0;
                            long activityTimestamp = 0;
                            if (filer.length() >= 8) {
                                clockTimestamp = FilerIO.readLong(filer, "clockTimestamp");
                            }
                            if (filer.length() >= 16) {
                                activityTimestamp = FilerIO.readLong(filer, "activityTimestamp");
                            }
                            sipReference.set(new Sip(clockTimestamp, activityTimestamp));
                        }
                    } else {
                        sipReference.set(new Sip(0, 0));
                    }
                    return null;
                }
            });
            sip = sipReference.get();
        }
        return sip;
    }

    @Override
    public boolean setSip(final Sip sip) throws IOException {
        return sipFilerProvider.readWriteAutoGrow(8L, new ChunkTransaction<Void, Boolean>() {
            @Override
            public Boolean commit(Void monkey, ChunkFiler filer, Object lock) throws IOException {
                Sip existingSip = sipReference.get();
                while (sip.compareTo(existingSip) > 0) {
                    if (sipReference.compareAndSet(existingSip, sip)) {
                        synchronized (lock) {
                            filer.seek(0);
                            FilerIO.writeLong(filer, sip.clockTimestamp, "clockTimestamp");
                            FilerIO.writeLong(filer, sip.activityTimestamp, "activityTimestamp");
                        }
                        return true;
                    } else {
                        existingSip = sipReference.get();
                    }
                }
                return false;
            }
        });
    }
}
