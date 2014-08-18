package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.googlecode.javaewah.BitmapStorage;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.query.RecoQuery;
import com.jivesoftware.os.miru.api.query.result.RecoResult;
import com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition;
import com.jivesoftware.os.miru.service.partition.MiruQueryHandle;
import com.jivesoftware.os.miru.service.partition.MiruRemoteHostedPartition;
import com.jivesoftware.os.miru.service.query.RecoReport;
import com.jivesoftware.os.miru.service.query.base.ExecuteMiruFilter;
import com.jivesoftware.os.miru.service.stream.MiruQueryStream;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class RecoExecuteQuery implements ExecuteQuery<RecoResult, RecoReport> {

    private final MiruFilterUtils utils;
    private final RecoQuery query;
    private final int bitsetBufferSize;

    public RecoExecuteQuery(MiruFilterUtils utils,
        RecoQuery query,
        int bitsetBufferSize) {
        this.utils = utils;
        this.query = query;
        this.bitsetBufferSize = bitsetBufferSize;
    }


    @Override
    public RecoResult executeLocal(MiruLocalHostedPartition partition, Optional<RecoReport> report) throws Exception {
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
            RecoResult reco = utils.collaborativeFiltering(stream, query, report, answer, bitsetBufferSize);

            return reco;
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public RecoResult executeRemote(MiruRemoteHostedPartition partition, Optional<RecoResult> lastResult) throws Exception {
        MiruReader miruReader = partition.getMiruReader();

        return miruReader.collaborativeFilteringRecommendations(partition.getPartitionId(), query, lastResult);
    }
}
