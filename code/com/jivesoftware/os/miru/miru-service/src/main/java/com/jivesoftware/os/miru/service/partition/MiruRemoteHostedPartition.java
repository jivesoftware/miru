package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.service.stream.MiruQueryStream;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/** @author jonathan */
public class MiruRemoteHostedPartition implements MiruHostedPartition {

    private final MiruPartitionCoord coord;
    private final MiruPartitionInfoProvider infoProvider;
    private final RequestHelper requestHelper;
    private final AtomicBoolean removed;

    public MiruRemoteHostedPartition(MiruPartitionCoord coord, MiruPartitionInfoProvider infoProvider, RequestHelper requestHelper) {
        this.coord = coord;
        this.infoProvider = infoProvider;
        this.requestHelper = requestHelper;
        this.removed = new AtomicBoolean(false);
    }

    @Override
    public void remove() {
        removed.compareAndSet(false, true);
    }

    @Override
    public boolean isLocal() {
        return false;
    }

    @Override
    public MiruPartitionCoord getCoord() {
        return coord;
    }

    @Override
    public MiruTenantId getTenantId() {
        return coord.tenantId;
    }

    @Override
    public MiruPartitionId getPartitionId() {
        return coord.partitionId;
    }

    @Override
    public MiruPartitionState getState() {
        Optional<MiruPartitionCoordInfo> infoOptional = infoProvider.get(coord);
        return infoOptional.isPresent() ? infoOptional.get().state : MiruPartitionState.offline;
    }

    @Override
    public MiruBackingStorage getStorage() {
        Optional<MiruPartitionCoordInfo> infoOptional = infoProvider.get(coord);
        return infoOptional.isPresent() ? infoOptional.get().storage : MiruBackingStorage.unknown;
    }

    @Override
    public void index(Iterator<MiruPartitionedActivity> activities) {
    }

    @Override
    public void warm() {
    }

    @Override
    public long sizeInMemory() throws Exception {
        return 0;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return 0;
    }

    @Override
    public void setStorage(MiruBackingStorage storage) {
    }

    @Override
    public MiruQueryHandle getQueryHandle() throws Exception {
        //TODO split local/remote handles
        return new MiruQueryHandle() {
            @Override
            public MiruQueryStream getQueryStream() {
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
            public RequestHelper getRequestHelper() {
                return requestHelper;
            }

            @Override
            public void close() throws Exception {
            }
        };
    }

    @Override
    public String toString() {
        return "MiruRemoteHostedPartition{" +
            "coord=" + coord +
            '}';
    }

}
