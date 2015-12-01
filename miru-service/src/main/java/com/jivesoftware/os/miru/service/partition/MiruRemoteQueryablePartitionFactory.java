package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.partition.MiruQueryablePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;

/** @author jonathan */
public class MiruRemoteQueryablePartitionFactory {

    public <BM extends IBM, IBM, S extends MiruSipCursor<S>> MiruQueryablePartition<BM, IBM> create(final MiruPartitionCoord coord) {

        return new MiruQueryablePartition<BM, IBM>() {

            @Override
            public boolean isLocal() {
                return false;
            }

            @Override
            public MiruPartitionCoord getCoord() {
                return coord;
            }

            @Override
            public MiruRequestHandle<BM, IBM, ?> inspectRequestHandle() throws Exception {
                throw new UnsupportedOperationException("Remote partitions cannot be inspected");
            }

            @Override
            public MiruRequestHandle<BM, IBM, S> acquireQueryHandle(StackBuffer stackBuffer) throws Exception {
                return new MiruRequestHandle<BM, IBM, S>() {

                    @Override
                    public MiruBitmaps<BM, IBM> getBitmaps() {
                        return null;
                    }

                    @Override
                    public MiruRequestContext<IBM, S> getRequestContext() {
                        return null;
                    }

                    @Override
                    public boolean isLocal() {
                        return false;
                    }

                    @Override
                    public boolean canBackfill() {
                        return false;
                    }

                    @Override
                    public MiruPartitionCoord getCoord() {
                        return coord;
                    }

                    @Override
                    public MiruPartitionState getState() {
                        return null;
                    }

                    @Override
                    public MiruBackingStorage getStorage() {
                        return null;
                    }

                    @Override
                    public void close() throws Exception {
                    }
                };
            }
        };
    }
}
