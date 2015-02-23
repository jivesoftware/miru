package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.filer.io.api.ChunkTransaction;
import java.io.IOException;

/**
 *
 * @param <H>
 * @param <M>
 */
public interface MiruFilerProvider<H, M> {

    <R> R read(H initialCapacity, ChunkTransaction<M, R> transaction) throws IOException;

    <R> R writeNewReplace(H initialCapacity, ChunkTransaction<M, R> transaction) throws IOException;

    <R> R readWriteAutoGrow(H initialCapacity, ChunkTransaction<M, R> transaction) throws IOException;
}
