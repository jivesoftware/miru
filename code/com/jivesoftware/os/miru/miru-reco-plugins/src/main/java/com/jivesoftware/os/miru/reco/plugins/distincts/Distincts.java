package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.map.store.api.KeyRange;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.index.TermIdStream;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;

/**
 *
 */
public class Distincts {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();
    private final MiruTermComposer termComposer;

    public Distincts(MiruTermComposer termComposer) {
        this.termComposer = termComposer;
    }

    public <BM> DistinctsAnswer gather(MiruBitmaps<BM> bitmaps,
        MiruRequestContext<BM> requestContext,
        final MiruRequest<DistinctsQuery> request,
        final Optional<DistinctsReport> lastReport,
        MiruSolutionLog solutionLog)
        throws Exception {

        log.debug("Gather distincts for query={}", request);

        int fieldId = requestContext.getSchema().getFieldId(request.query.aggregateCountAroundField);
        final MiruFieldDefinition fieldDefinition = requestContext.getSchema().getFieldDefinition(fieldId);
        List<String> prefixes = request.query.prefixes;
        List<KeyRange> ranges = null;
        if (prefixes != null) {
            ranges = Lists.newArrayListWithCapacity(prefixes.size());
            for (String prefix : prefixes) {
                ranges.add(new KeyRange(
                    termComposer.prefixLowerInclusive(fieldDefinition.prefix, prefix),
                    termComposer.prefixUpperExclusive(fieldDefinition.prefix, prefix)));
            }
        }

        final List<String> results = Lists.newArrayList();
        requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary).streamTermIdsForField(fieldId, ranges, new TermIdStream() {
            @Override
            public boolean stream(MiruTermId termId) {
                results.add(termComposer.decompose(fieldDefinition, termId));
                return true;
            }
        });

        boolean resultsExhausted = request.query.timeRange.smallestTimestamp > requestContext.getTimeIndex().getLargestTimestamp();
        int collectedDistincts = results.size();
        DistinctsAnswer result = new DistinctsAnswer(results, collectedDistincts, resultsExhausted);
        log.debug("result={}", result);
        return result;
    }

}
