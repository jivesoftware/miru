package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.googlecode.javaewah.BitmapStorage;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.query.TrendingQuery;
import com.jivesoftware.os.miru.api.query.result.TrendingResult;
import com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition;
import com.jivesoftware.os.miru.service.partition.MiruQueryHandle;
import com.jivesoftware.os.miru.service.partition.MiruRemoteHostedPartition;
import com.jivesoftware.os.miru.service.query.TrendingReport;
import com.jivesoftware.os.miru.service.query.base.ExecuteMiruFilter;
import com.jivesoftware.os.miru.service.stream.MiruQueryStream;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class TrendingExecuteQuery implements ExecuteQuery<TrendingResult, TrendingReport> {

    private final MiruFilterUtils utils;
    private final TrendingQuery query;
    private final int bitsetBufferSize;

    public TrendingExecuteQuery(MiruFilterUtils utils,
        TrendingQuery query,
        int bitsetBufferSize) {
        this.utils = utils;
        this.query = query;
        this.bitsetBufferSize = bitsetBufferSize;
    }

    @Override
    public TrendingResult executeLocal(MiruLocalHostedPartition partition, Optional<TrendingReport> report) throws Exception {
        try (MiruQueryHandle handle = partition.getQueryHandle()) {

            MiruQueryStream stream = handle.getQueryStream();

            // Start building up list of bitmap operations to run
            List<EWAHCompressedBitmap> ands = new ArrayList<>();

            // 1) Execute the combined filter above on the given stream, add the bitmap
            ExecuteMiruFilter executeMiruFilter = new ExecuteMiruFilter(stream.schema, stream.fieldIndex, stream.executorService,
                query.constraintsFilter, Optional.<BitmapStorage>absent(), -1, bitsetBufferSize);
            ands.add(executeMiruFilter.call());

            // 2) Add in the authz check if we have it
            if (query.authzExpression.isPresent()) {
                ands.add(stream.authzIndex.getCompositeAuthz(query.authzExpression.get()));
            }

            // 3) Mask out anything that hasn't made it into the activityIndex yet, orToSourceSize that has been removed from the index
            ands.add(utils.buildIndexMask(stream.activityIndex.lastId(), Optional.of(stream.removalIndex.getIndex())));

            // AND it all together and return the results
            EWAHCompressedBitmap answer = utils.bufferedAnd(ands, bitsetBufferSize);
            TrendingResult trending = utils.trending(stream, query, report, answer);

            return trending;
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public TrendingResult executeRemote(MiruRemoteHostedPartition partition, Optional<TrendingResult> lastResult) throws Exception {
        MiruReader miruReader = partition.getMiruReader();

        return miruReader.scoreTrending(partition.getPartitionId(), query, lastResult);
    }
}
