package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.filer.io.api.ChunkTransaction;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import java.io.IOException;

/**
 *
 * @param <H>
 * @param <M>
 */
public interface MiruFilerProvider<H, M> {

    <R> R read(H initialCapacity, ChunkTransaction<M, R> transaction, StackBuffer stackBuffer) throws IOException;

    <R> R writeNewReplace(H initialCapacity, ChunkTransaction<M, R> transaction, StackBuffer stackBuffer) throws IOException;

    <R> R readWriteAutoGrow(H initialCapacity, ChunkTransaction<M, R> transaction, StackBuffer stackBuffer) throws IOException;
}
