package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.Set;

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
        MiruRequestContext<BM, ?> requestContext,
        final DistinctsQuery query,
        MiruSolutionLog solutionLog)
        throws Exception {

        log.debug("Gather distincts for query={}", query);

        int fieldId = requestContext.getSchema().getFieldId(query.gatherDistinctsForField);
        final MiruFieldDefinition fieldDefinition = requestContext.getSchema().getFieldDefinition(fieldId);

        final List<String> results = Lists.newArrayList();
        if (requestContext.getTimeIndex().intersects(query.timeRange)) {
            if (MiruFilter.NO_FILTER.equals(query.constraintsFilter)) {
                List<KeyRange> ranges = null;
                if (query.prefixes != null && !query.prefixes.isEmpty()) {
                    ranges = Lists.newArrayListWithCapacity(query.prefixes.size());
                    for (String prefix : query.prefixes) {
                        ranges.add(new KeyRange(
                            termComposer.prefixLowerInclusive(fieldDefinition.prefix, prefix),
                            termComposer.prefixUpperExclusive(fieldDefinition.prefix, prefix)));
                    }
                }

                requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary).streamTermIdsForField(fieldId, ranges, termId -> {
                    results.add(termComposer.decompose(fieldDefinition, termId));
                    return true;
                });
            } else {
                final byte[][] prefixesAsBytes;
                if (query.prefixes != null) {
                    prefixesAsBytes = new byte[query.prefixes.size()][];
                    int i = 0;
                    for (String prefix : query.prefixes) {
                        prefixesAsBytes[i++] = termComposer.prefixLowerInclusive(fieldDefinition.prefix, prefix);
                    }
                } else {
                    prefixesAsBytes = new byte[0][];
                }

                List<BM> ands = Lists.newArrayList();
                BM constrained = bitmaps.create();
                aggregateUtil.filter(bitmaps, requestContext.getSchema(), termComposer, requestContext.getFieldIndexProvider(), query.constraintsFilter,
                    solutionLog, constrained, null, requestContext.getActivityIndex().lastId(), -1);
                ands.add(constrained);

                if (!MiruTimeRange.ALL_TIME.equals(query.timeRange)) {
                    MiruTimeRange timeRange = query.timeRange;
                    ands.add(bitmaps.buildTimeRangeMask(requestContext.getTimeIndex(), timeRange.smallestTimestamp, timeRange.largestTimestamp));
                }

                BM result;
                if (ands.size() == 1) {
                    result = ands.get(0);
                } else {
                    result = bitmaps.create();
                    bitmaps.and(result, ands);
                }

                Set<MiruTermId> termIds = Sets.newHashSet();
                //TODO expose batch size to query?
                aggregateUtil.gather(bitmaps, requestContext, result, fieldId, 100, termIds);

                if (prefixesAsBytes.length > 0) {
                    termIds = Sets.filter(termIds, input -> {
                        if (input != null) {
                            byte[] termBytes = input.getBytes();
                            for (byte[] prefixAsBytes : prefixesAsBytes) {
                                if (arrayStartsWith(termBytes, prefixAsBytes)) {
                                    return true;
                                }
                            }
                        }
                        return false;
                    });
                }

                results.addAll(Collections2.transform(termIds, input -> termComposer.decompose(fieldDefinition, input)));
            }
        }

        boolean resultsExhausted = query.timeRange.smallestTimestamp > requestContext.getTimeIndex().getLargestTimestamp();
        int collectedDistincts = results.size();
        DistinctsAnswer result = new DistinctsAnswer(results, collectedDistincts, resultsExhausted);
        log.debug("result={}", result);
        return result;
    }

    private boolean arrayStartsWith(byte[] termBytes, byte[] prefixAsBytes) {
        if (termBytes.length < prefixAsBytes.length) {
            return false;
        }
        for (int i = 0; i < prefixAsBytes.length; i++) {
            if (termBytes[i] != prefixAsBytes[i]) {
                return false;
            }
        }
        return true;
    }
}
