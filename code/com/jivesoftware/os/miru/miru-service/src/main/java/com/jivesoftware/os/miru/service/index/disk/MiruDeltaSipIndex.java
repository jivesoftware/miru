package com.jivesoftware.os.miru.service.index.disk;

import com.jivesoftware.os.miru.plugin.index.MiruSipIndex;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader.Sip;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * DELTA FORCE
 */
public class MiruDeltaSipIndex implements MiruSipIndex {

    private final MiruSipIndex backingIndex;
    private final AtomicReference<Sip> sipReference = new AtomicReference<>();

    public MiruDeltaSipIndex(MiruSipIndex backingIndex) {
        this.backingIndex = backingIndex;
    }

    @Override
    public Sip getSip() throws IOException {
        Sip sip = sipReference.get();
        if (sip == null) {
            return backingIndex.getSip();
        }
        return sip;
    }

    @Override
    public boolean setSip(final Sip sip) throws IOException {
        Sip existing = sipReference.get();
        if (existing == null) {
            existing = backingIndex.getSip();
        }
        while (sip.compareTo(existing) > 0) {
            if (sipReference.compareAndSet(existing, sip)) {
                return true;
            } else {
                existing = sipReference.get();
            }
        }
        return false;
    }

    public void merge() throws Exception {
        Sip sip = sipReference.get();
        if (sip != null) {
            backingIndex.setSip(sip);
        }
    }
}
