package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.map.store.api.KeyRange;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.index.TermIdStream;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruTermCount;
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

        final List<String> results = Lists.newArrayList();
        if (MiruFilter.NO_FILTER.equals(request.query.constraintsFilter)) {
            List<KeyRange> ranges = null;
            if (request.query.prefixes != null) {
                ranges = Lists.newArrayListWithCapacity(request.query.prefixes.size());
                for (String prefix : request.query.prefixes) {
                    ranges.add(new KeyRange(
                        termComposer.prefixLowerInclusive(fieldDefinition.prefix, prefix),
                        termComposer.prefixUpperExclusive(fieldDefinition.prefix, prefix)));
                }
            }

            requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary).streamTermIdsForField(fieldId, ranges, new TermIdStream() {
                @Override
                public boolean stream(MiruTermId termId) {
                    results.add(termComposer.decompose(fieldDefinition, termId));
                    return true;
                }
            });
        } else {
            final byte[][] prefixesAsBytes;
            if (request.query.prefixes != null) {
                prefixesAsBytes = new byte[request.query.prefixes.size()][];
                int i = 0;
                for (String prefix : request.query.prefixes) {
                    prefixesAsBytes[i++] = termComposer.prefixLowerInclusive(fieldDefinition.prefix, prefix);
                }
            } else {
                prefixesAsBytes = new byte[0][];
            }

            BM result = bitmaps.create();
            aggregateUtil.filter(bitmaps, requestContext.getSchema(), termComposer, requestContext.getFieldIndexProvider(), request.query.constraintsFilter,
                solutionLog, result, -1);
            aggregateUtil.stream(bitmaps, request.tenantId, requestContext, result, Optional.<BM>absent(), fieldId, request.query.aggregateCountAroundField,
                new CallbackStream<MiruTermCount>() {
                    @Override
                    public MiruTermCount callback(MiruTermCount termCount) throws Exception {
                        if (termCount != null) {
                            if (prefixesAsBytes.length == 0) {
                                results.add(termComposer.decompose(fieldDefinition, termCount.termId));
                            } else {
                                byte[] termBytes = termCount.termId.getBytes();
                                for (byte[] prefixAsBytes : prefixesAsBytes) {
                                    if (arrayStartsWith(termBytes, prefixAsBytes)) {
                                        results.add(termComposer.decompose(fieldDefinition, termCount.termId));
                                        break;
                                    }
                                }
                            }
                        }
                        return termCount;
                    }
                });
        }

        boolean resultsExhausted = request.query.timeRange.smallestTimestamp > requestContext.getTimeIndex().getLargestTimestamp();
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
