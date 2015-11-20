package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.partition.MiruQueryablePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;

/** @author jonathan */
public class MiruRemoteQueryablePartitionFactory {

    public <BM, S extends MiruSipCursor<S>> MiruQueryablePartition<BM> create(final MiruPartitionCoord coord) {

        return new MiruQueryablePartition<BM>() {

            @Override
            public boolean isLocal() {
                return false;
            }

            @Override
            public MiruPartitionCoord getCoord() {
                return coord;
            }

            @Override
            public MiruRequestHandle<BM, ?> inspectRequestHandle() throws Exception {
                throw new UnsupportedOperationException("Remote partitions cannot be inspected");
            }

            @Override
            public MiruRequestHandle<BM, S> acquireQueryHandle(byte[] primitiveBuffer) throws Exception {
                return new MiruRequestHandle<BM, S>() {

                    @Override
                    public MiruBitmaps<BM> getBitmaps() {
                        return null;
                    }

                    @Override
                    public MiruRequestContext<BM, S> getRequestContext() {
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
                    public void close() throws Exception {
                    }
                };
            }
        };
    }
}
