package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.index.TermIdStream;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
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

    public <BM extends IBM, IBM> DistinctsAnswer gather(MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> requestContext,
        final DistinctsQuery query,
        int gatherBatchSize,
        StackBuffer stackBuffer,
        MiruSolutionLog solutionLog)
        throws Exception {

        MiruSchema schema = requestContext.getSchema();
        int fieldId = schema.getFieldId(query.gatherDistinctsForField);
        MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(fieldId);

        List<MiruValue> results = Lists.newArrayList();
        gatherDirect(bitmaps, requestContext, query, gatherBatchSize, solutionLog,
            termId -> results.add(new MiruValue(termComposer.decompose(schema, fieldDefinition, stackBuffer, termId))));

        boolean resultsExhausted = query.timeRange.smallestTimestamp > requestContext.getTimeIndex().getLargestTimestamp();
        int collectedDistincts = results.size();
        DistinctsAnswer result = new DistinctsAnswer(results, collectedDistincts, resultsExhausted);
        log.debug("result={}", result);
        return result;
    }

    public <BM extends IBM, IBM> void gatherDirect(MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> requestContext,
        DistinctsQuery query,
        int gatherBatchSize,
        MiruSolutionLog solutionLog,
        TermIdStream termIdStream) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();

        log.debug("Gather distincts for query={}", query);

        MiruSchema schema = requestContext.getSchema();
        int fieldId = schema.getFieldId(query.gatherDistinctsForField);
        MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(fieldId);

        if (requestContext.getTimeIndex().intersects(query.timeRange)) {
            if (MiruFilter.NO_FILTER.equals(query.constraintsFilter)) {
                List<KeyRange> ranges = null;
                if (query.prefixes != null && !query.prefixes.isEmpty()) {
                    ranges = Lists.newArrayListWithCapacity(query.prefixes.size());
                    for (MiruValue prefix : query.prefixes) {
                        ranges.add(new KeyRange(
                            termComposer.prefixLowerInclusive(schema, fieldDefinition, stackBuffer, prefix.parts),
                            termComposer.prefixUpperExclusive(schema, fieldDefinition, stackBuffer, prefix.parts)));
                    }
                }

                requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary)
                    .streamTermIdsForField(fieldId, ranges, termIdStream, stackBuffer);
            } else {
                long start = System.currentTimeMillis();
                final byte[][] prefixesAsBytes;
                if (query.prefixes != null) {
                    prefixesAsBytes = new byte[query.prefixes.size()][];
                    int i = 0;
                    for (MiruValue prefix : query.prefixes) {
                        prefixesAsBytes[i++] = termComposer.prefixLowerInclusive(schema, fieldDefinition,  stackBuffer, prefix.parts);
                    }
                } else {
                    prefixesAsBytes = new byte[0][];
                }

                List<IBM> ands = Lists.newArrayList();
                BM constrained = aggregateUtil.filter(bitmaps, schema, termComposer, requestContext.getFieldIndexProvider(),
                    query.constraintsFilter, solutionLog, null, requestContext.getActivityIndex().lastId(stackBuffer), -1, stackBuffer);
                ands.add(constrained);

                if (!MiruTimeRange.ALL_TIME.equals(query.timeRange)) {
                    MiruTimeRange timeRange = query.timeRange;
                    ands.add(bitmaps.buildTimeRangeMask(requestContext.getTimeIndex(), timeRange.smallestTimestamp, timeRange.largestTimestamp, stackBuffer));
                }

                BM result;
                if (ands.size() == 1) {
                    result = bitmaps.copy(ands.get(0));
                } else {
                    result = bitmaps.and(ands);
                }
                solutionLog.log(MiruSolutionLogLevel.INFO, "distincts gatherDirect: setup {} ms.", System.currentTimeMillis() - start);

                start = System.currentTimeMillis();
                //TODO expose batch size to query?
                aggregateUtil.gather(bitmaps, requestContext, result, fieldId, gatherBatchSize, solutionLog, termId -> {
                    if (prefixesAsBytes.length > 0) {
                        byte[] termBytes = termId.getBytes();
                        for (byte[] prefixAsBytes : prefixesAsBytes) {
                            if (arrayStartsWith(termBytes, prefixAsBytes)) {
                                return termIdStream.stream(termId);
                            }
                        }
                        return true;
                    } else {
                        return termIdStream.stream(termId);
                    }
                }, stackBuffer);
                solutionLog.log(MiruSolutionLogLevel.INFO, "distincts gatherDirect: gather {} ms.", System.currentTimeMillis() - start);
            }
        }
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
