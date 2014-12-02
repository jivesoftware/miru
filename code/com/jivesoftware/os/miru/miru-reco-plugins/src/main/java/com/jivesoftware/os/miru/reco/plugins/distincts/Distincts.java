package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import java.util.List;

/**
 *
 */
public class Distincts {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public <BM> DistinctsAnswer gather(MiruBitmaps<BM> bitmaps,
        MiruRequestContext<BM> requestContext,
        final MiruRequest<DistinctsQuery> request,
        final Optional<DistinctsReport> lastReport,
        MiruSolutionLog solutionLog)
        throws Exception {

        log.debug("Gather distincts for query={}", request);

        int fieldId = requestContext.getSchema().getFieldId(request.query.aggregateCountAroundField);

        List<MiruTermId> results = Lists.newArrayList(requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary).getTermIdsForField(fieldId));

        boolean resultsExhausted = request.query.timeRange.smallestTimestamp >= requestContext.getTimeIndex().getSmallestTimestamp();
        int collectedDistincts = results.size();
        DistinctsAnswer result = new DistinctsAnswer(ImmutableList.copyOf(results), collectedDistincts, resultsExhausted);
        log.debug("result={}", result);
        return result;
    }

}
