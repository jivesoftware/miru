package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.filer.io.api.ChunkTransaction;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import java.io.IOException;

/**
 *
 */
public class KeyedFilerProvider<H, M> implements MiruFilerProvider<H, M> {

    private final KeyedFilerStore<H, M> keyedFilerStore;
    private final byte[] key;

    public KeyedFilerProvider(KeyedFilerStore<H, M> keyedFilerStore, byte[] key) {
        this.keyedFilerStore = keyedFilerStore;
        this.key = key;
    }

    @Override
    public <R> R read(H initialCapacity, final ChunkTransaction<M, R> transaction, StackBuffer stackBuffer) throws IOException, InterruptedException {
        return keyedFilerStore.read(key, initialCapacity, transaction, stackBuffer);
    }

    @Override
    public <R> R writeNewReplace(H initialCapacity, final ChunkTransaction<M, R> transaction, StackBuffer stackBuffer) throws IOException, InterruptedException {
        return keyedFilerStore.writeNewReplace(key, initialCapacity, transaction, stackBuffer);
    }

    @Override
    public <R> R readWriteAutoGrow(H initialCapacity, final ChunkTransaction<M, R> transaction, StackBuffer stackBuffer) throws IOException,
        InterruptedException {
        return keyedFilerStore.readWriteAutoGrow(key, initialCapacity, transaction, stackBuffer);
    }

}
