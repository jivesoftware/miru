package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult;
import com.jivesoftware.os.miru.api.query.result.DistinctCountResult;
import com.jivesoftware.os.miru.api.query.result.TrendingResult;
import com.jivesoftware.os.miru.service.query.AggregateCountsReport;
import com.jivesoftware.os.miru.service.query.DistinctCountReport;
import com.jivesoftware.os.miru.service.query.TrendingReport;
import com.jivesoftware.os.miru.service.stream.factory.ExecuteQuery;
import com.jivesoftware.os.miru.service.stream.factory.MiruStreamCollector;
import com.jivesoftware.os.miru.service.stream.factory.remote.RemoteCountCollector;
import com.jivesoftware.os.miru.service.stream.factory.remote.RemoteFilterCollector;
import com.jivesoftware.os.miru.service.stream.factory.remote.RemoteTrendingCollector;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/** @author jonathan */
public class MiruRemoteHostedPartition implements MiruHostedPartition {

    private final MiruPartitionCoord coord;
    private final MiruPartitionInfoProvider infoProvider;
    private final MiruReader miruReader;
    private final AtomicBoolean removed;

    public MiruRemoteHostedPartition(MiruPartitionCoord coord, MiruPartitionInfoProvider infoProvider, MiruReader miruReader) {
        this.coord = coord;
        this.infoProvider = infoProvider;
        this.miruReader = miruReader;
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

    public MiruReader getMiruReader() {
        return miruReader;
    }

    @Override
    public MiruStreamCollector<AggregateCountsResult> createFilterCollector(ExecuteQuery<AggregateCountsResult, AggregateCountsReport> executeQuery) {
        return new RemoteFilterCollector(this, executeQuery);
    }

    @Override
    public MiruStreamCollector<DistinctCountResult> createCountCollector(ExecuteQuery<DistinctCountResult, DistinctCountReport> executeQuery) {
        return new RemoteCountCollector(this, executeQuery);
    }

    @Override
    public MiruStreamCollector<TrendingResult> createTrendingCollector(ExecuteQuery<TrendingResult, TrendingReport> executeQuery) {
        return new RemoteTrendingCollector(this, executeQuery);
    }

    @Override
    public String toString() {
        return "MiruRemoteHostedPartition{" +
            "coord=" + coord +
            '}';
    }
}
