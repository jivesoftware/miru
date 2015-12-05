package com.jivesoftware.os.miru.plugin.index;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;

/**
 *
 */
public interface MiruGetIndex<BM> {

    /**
     * Gets the raw index in a safe manner. The returned index is guaranteed to be free of concurrent writes,
     * so this method is safe to use even if the calling thread does not hold the context's write lock.
     *
     * @return the raw safe index
     * @throws Exception
     */
    Optional<BM> getIndex(StackBuffer stackBuffer) throws Exception;
}
