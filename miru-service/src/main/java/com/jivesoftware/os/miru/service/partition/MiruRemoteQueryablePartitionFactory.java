package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.partition.MiruQueryablePartition;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import java.util.concurrent.ExecutorService;

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
            public MiruPartitionState getState() {
                return null;
            }

            @Override
            public MiruBackingStorage getStorage() {
                return null;
            }

            @Override
            public MiruRequestHandle<BM, IBM, ?> inspectRequestHandle(boolean hotDeploy) throws Exception {
                throw new UnsupportedOperationException("Remote partitions cannot be inspected");
            }

            @Override
            public boolean isAvailable() {
                return false;
            }

            @Override
            public MiruRequestHandle<BM, IBM, S> acquireQueryHandle() throws Exception {
                return new MiruRequestHandle<BM, IBM, S>() {

                    @Override
                    public MiruBitmaps<BM, IBM> getBitmaps() {
                        return null;
                    }

                    @Override
                    public MiruRequestContext<BM, IBM, S> getRequestContext() {
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
                    public TrackError getTrackError() {
                        return null;
                    }

                    @Override
                    public void close() throws Exception {
                    }

                    @Override
                    public void submit(ExecutorService executorService, MiruRequestHandle.AsyncQuestion<BM, IBM> asyncQuestion) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void acquireChitsAndMerge(String name, long batchSize) {
                    }

                    @Override
                    public void compact() {
                    }
                };
            }
        };
    }
}
