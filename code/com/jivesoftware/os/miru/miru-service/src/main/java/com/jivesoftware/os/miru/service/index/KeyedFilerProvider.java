package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.filer.keyed.store.KeyedFilerStore;
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

    public <R> R execute(long initialCapacity, final FilerTransaction<Filer, R> filerTransaction) throws IOException {
        return keyedFilerStore.execute(key, initialCapacity, filerTransaction);
    }
}
