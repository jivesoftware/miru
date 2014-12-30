package com.jivesoftware.os.miru.service.index.memory;

import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class ReadWrite<BM> {

    public final AtomicReference<BM> read;
    public final AtomicReference<BM> write;

    public boolean needsMerge = false;
    public int lastId;

    public ReadWrite(BM initialBitmap, int lastId) {
        this.read = new AtomicReference<>(initialBitmap);
        this.write = new AtomicReference<>();
        this.lastId = lastId;
    }
}
