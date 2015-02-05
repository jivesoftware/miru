package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.filer.map.store.api.KeyedFilerStore;
import java.io.IOException;

/**
 *
 */
public class KeyedFilerProvider implements MiruFilerProvider {

    private final KeyedFilerStore keyedFilerStore;
    private final byte[] key;

    public KeyedFilerProvider(KeyedFilerStore keyedFilerStore, byte[] key) {
        this.keyedFilerStore = keyedFilerStore;
        this.key = key;
    }

    @Override
    public <R> R read(long initialCapacity, final FilerTransaction<Filer, R> filerTransaction) throws IOException {
        return keyedFilerStore.read(key, initialCapacity, filerTransaction);
    }

    @Override
    public <R> R writeNewReplace(long initialCapacity, final FilerTransaction<Filer, R> filerTransaction) throws IOException {
        return keyedFilerStore.writeNewReplace(key, initialCapacity, filerTransaction);
    }

    @Override
    public <R> R readWriteAutoGrow(long initialCapacity, final FilerTransaction<Filer, R> filerTransaction) throws IOException {
        return keyedFilerStore.readWriteAutoGrow(key, initialCapacity, filerTransaction);
    }

}
