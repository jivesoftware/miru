package com.jivesoftware.os.miru.service.index.filer;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndex;
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader.Sip;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class MiruFilerSipIndex implements MiruSipIndex {

    private final MiruFilerProvider sipFilerProvider;
    private final AtomicReference<Sip> sipReference = new AtomicReference<>();

    public MiruFilerSipIndex(MiruFilerProvider sipFilerProvider) {
        this.sipFilerProvider = sipFilerProvider;
    }

    @Override
    public Sip getSip() throws IOException {
        Sip sip = sipReference.get();
        if (sip == null) {
            sipFilerProvider.read(-1, new FilerTransaction<Filer, Void>() {
                @Override
                public Void commit(Object lock, Filer filer) throws IOException {
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
        return sipFilerProvider.readWriteAutoGrow(8, new FilerTransaction<Filer, Boolean>() {
            @Override
            public Boolean commit(Object lock, Filer filer) throws IOException {
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
