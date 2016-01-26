package com.jivesoftware.os.miru.service.index.delta;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndex;
import com.jivesoftware.os.miru.service.index.Mergeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * DELTA FORCE
 */
public class MiruDeltaSipIndex<S extends MiruSipCursor<S>> implements MiruSipIndex<S>, Mergeable {

    private final MiruSipIndex<S> backingIndex;
    private final AtomicReference<S> sipReference = new AtomicReference<>();

    public MiruDeltaSipIndex(MiruSipIndex<S> backingIndex) {
        this.backingIndex = backingIndex;
    }

    @Override
    public Optional<S> getSip(StackBuffer stackBuffer) throws IOException, InterruptedException {
        S sip = sipReference.get();
        if (sip == null) {
            return backingIndex.getSip(stackBuffer);
        }
        return Optional.fromNullable(sip);
    }

    @Override
    public boolean setSip(final S sip, StackBuffer stackBuffer) throws IOException, InterruptedException {
        S existing = sipReference.get();
        if (existing == null) {
            existing = backingIndex.getSip(stackBuffer).orNull();
            if (!sipReference.compareAndSet(null, existing)) {
                existing = sipReference.get();
            }
        }
        if (existing == null) {
            sipReference.set(sip);
            return true;
        } else {
            while (sip.compareTo(existing) > 0) {
                if (sipReference.compareAndSet(existing, sip)) {
                    return true;
                } else {
                    existing = sipReference.get();
                }
            }
            return false;
        }
    }

    @Override
    public void merge(MiruSchema schema, StackBuffer stackBuffer) throws Exception {
        S sip = sipReference.get();
        if (sip != null) {
            backingIndex.setSip(sip, stackBuffer);
        }
    }
}
