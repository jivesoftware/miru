package com.jivesoftware.os.miru.stumptown.plugins;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.stumptown.plugins.StumptownAnswer.Waveform;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class Stumptown {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruProvider miruProvider;

    public Stumptown(MiruProvider miruProvider) {
        this.miruProvider = miruProvider;
    }

    public <BM> Waveform stumptowning(MiruBitmaps<BM> bitmaps,
        MiruRequestContext<BM, ?> requestContext,
        MiruTenantId tenantId,
        BM answer,
        int desiredNumberOfResults,
        int[] indexes)
        throws Exception {

        log.debug("Get stumptowning for answer={}", answer);

        MiruActivityInternExtern internExtern = miruProvider.getActivityInternExtern(tenantId);
        MiruSchema schema = requestContext.getSchema();

        List<MiruActivity> results = Lists.newArrayListWithCapacity(desiredNumberOfResults);
        long cardinality = bitmaps.cardinality(answer);
        MiruIntIterator iter = bitmaps.intIterator(answer);
        for (long i = 0; i < cardinality && iter.hasNext(); i++) {
            int index = iter.next();
            if (i > (cardinality - 1 - desiredNumberOfResults)) {
                results.add(internExtern.extern(requestContext.getActivityIndex().get(tenantId, index), schema));
            }
        }
        Collections.reverse(results); // chronologically descending (for proper alignment when merging/appending older partitions)

        long[] waveform = new long[indexes.length - 1];
        bitmaps.boundedCardinalities(answer, indexes, waveform);
        return new Waveform(waveform, results);
    }
}
