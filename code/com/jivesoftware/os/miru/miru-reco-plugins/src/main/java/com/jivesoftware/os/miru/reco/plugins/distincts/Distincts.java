package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class Distincts {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    public <BM> DistinctsAnswer gather(MiruBitmaps<BM> bitmaps,
        MiruRequestContext<BM> requestContext,
        MiruRequest<DistinctsQuery> request,
        BM answer,
        Optional<DistinctsReport> lastReport,
        MiruSolutionLog solutionLog)
        throws Exception {

        log.debug("Gather distincts for query={}", request);

        int fieldId = requestContext.getSchema().getFieldId(request.query.distinctsAroundField);

        List<MiruTermId> results = Lists.newArrayList();
        MiruIntIterator iter = bitmaps.intIterator(answer);
        while (iter.hasNext()) {
            int index = iter.next();
            results.addAll(Arrays.asList(requestContext.getActivityIndex().get(request.tenantId, index, fieldId)));
        }

        boolean resultsExhausted = request.query.timeRange.smallestTimestamp > requestContext.getTimeIndex().getLargestTimestamp();
        int collectedDistincts = results.size();
        DistinctsAnswer result = new DistinctsAnswer(results, collectedDistincts, resultsExhausted);
        log.debug("result={}", result);
        return result;
    }

}
