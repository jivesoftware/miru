package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionState;
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
import java.util.Iterator;

/**
 *
 * @author jonathan
 */
public interface MiruHostedPartition {

    void remove() throws Exception;

    boolean isLocal();

    MiruPartitionCoord getCoord();

    MiruTenantId getTenantId();

    MiruPartitionId getPartitionId();

    MiruPartitionState getState();

    MiruBackingStorage getStorage();

    void index(Iterator<MiruPartitionedActivity> activities) throws Exception;

    void warm();

    long sizeInMemory() throws Exception;

    long sizeOnDisk() throws Exception;

    void setStorage(MiruBackingStorage storage) throws Exception;

    MiruStreamCollector<AggregateCountsResult> createFilterCollector(ExecuteQuery<AggregateCountsResult, AggregateCountsReport> executeQuery);

    MiruStreamCollector<DistinctCountResult> createCountCollector(ExecuteQuery<DistinctCountResult, DistinctCountReport> executeQuery);

    MiruStreamCollector<TrendingResult> createTrendingCollector(ExecuteQuery<TrendingResult, TrendingReport> executeQuery);
}
