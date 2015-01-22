package com.jivesoftware.os.miru.reco.plugins.reco;

import com.google.common.base.Optional;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.hash.Hashing;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.BloomIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruIndexUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTermCount;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoAnswer.Recommendation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang.mutable.MutableInt;

/**
 *
 */
public class CollaborativeFiltering {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruAggregateUtil aggregateUtil;
    private final MiruIndexUtil indexUtil;

    public CollaborativeFiltering(MiruAggregateUtil aggregateUtil, MiruIndexUtil indexUtil) {
        this.aggregateUtil = aggregateUtil;
        this.indexUtil = indexUtil;
    }

    /*
     * I have viewed these things; among others who have also viewed these things, what have they viewed that I have not?
     */
    public <BM> RecoAnswer collaborativeFiltering(MiruSolutionLog solutionLog,
        MiruBitmaps<BM> bitmaps,
        MiruRequestContext<BM> requestContext,
        final MiruRequest<RecoQuery> request,
        Optional<RecoReport> report,
        BM answer)
        throws Exception {

        log.debug("Get collaborative filtering for answer={} query={}", answer, request);

        // answer: latest time I touched distinct parents <field1>

        // contributors: all latest distinct users <field2> for the latest distinct parents <field1>
        BM contributors = possibleContributors(bitmaps, requestContext, request, answer);
        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "contributors {}.", bitmaps.cardinality(contributors));
            solutionLog.log(MiruSolutionLogLevel.TRACE, "contributors bitmap {}", contributors);
        }

        // otherContributors: all *except me* of the latest distinct users <field2> against distinct parents <field1>
        BM otherContributors = bitmaps.create();
        bitmaps.andNot(otherContributors, contributors, Collections.singletonList(answer));

        // contributorHeap: ranked users <field2> based on cardinality of interactions with distinct parents <field1>
        MinMaxPriorityQueue<MiruTermCount> contributorHeap = rankContributors(bitmaps, request, requestContext, otherContributors);
        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "not my self {}.", bitmaps.cardinality(otherContributors));
            solutionLog.log(MiruSolutionLogLevel.TRACE, "not my self bitmap {}", otherContributors);
        }

        // contributions: all latest distinct parents <field3> for each distinct user <field2>
        BM contributions = contributions(bitmaps, contributorHeap, requestContext, request.query);
        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "contributions {}.", bitmaps.cardinality(contributions));
            solutionLog.log(MiruSolutionLogLevel.TRACE, "contributions bitmap {}", contributions);
        }

        final List<MiruTermCount> mostLike = new ArrayList<>(contributorHeap);
        final BloomIndex<BM> bloomIndex = new BloomIndex<>(bitmaps, Hashing.murmur3_128(), 100_000, 0.01f); // TODO fix somehow
        final List<BloomIndex.Mights<MiruTermCount>> wantBits = bloomIndex.wantBits(mostLike);
        // TODO handle authz

        // othersContributions: all latest distinct parents <field3> *except what I've touched* for each distinct user <field2>
        BM othersContributions = bitmaps.create();
        bitmaps.andNot(othersContributions, contributions, Collections.singletonList(contributors));
        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "othersContributions {}.", bitmaps.cardinality(othersContributions));
            solutionLog.log(MiruSolutionLogLevel.TRACE, "othersContributions bitmap {}", othersContributions);
        }

        BM scorable = othersContributions;
        MiruFilter constrainScorableFilter = request.query.constrainResults;
        if (!MiruFilter.NO_FILTER.equals(constrainScorableFilter)) {
            BM possible = bitmaps.create();
            aggregateUtil.filter(bitmaps, requestContext.getSchema(), requestContext.getFieldIndexProvider(),
                constrainScorableFilter, solutionLog, possible, -1);
            if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
                solutionLog.log(MiruSolutionLogLevel.INFO, "possible {}.", bitmaps.cardinality(possible));
                solutionLog.log(MiruSolutionLogLevel.TRACE, "possible bitmap {}", possible);
            }

            scorable = bitmaps.create();
            bitmaps.and(scorable, Arrays.asList(possible, othersContributions));
            if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
                solutionLog.log(MiruSolutionLogLevel.INFO, "scorable {}.", bitmaps.cardinality(scorable));
                solutionLog.log(MiruSolutionLogLevel.TRACE, "scorable bitmap {}", scorable);
            }
        }

        return score(bitmaps, request, scorable, requestContext, bloomIndex, wantBits);

    }

    private <BM> BM contributions(MiruBitmaps<BM> bitmaps, MinMaxPriorityQueue<MiruTermCount> userHeap, MiruRequestContext<BM> requestContext, RecoQuery query)
        throws Exception {

        int fieldId = requestContext.getSchema().getFieldId(query.lookupFieldNamed2);
        MiruFieldIndex<BM> fieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.pairedLatest);

        List<BM> toBeORed = new ArrayList<>();
        for (MiruTermCount tc : userHeap) {
            Optional<BM> index = fieldIndex.get(
                fieldId,
                indexUtil.makePairedLatestTerm(tc.termId, query.aggregateFieldName3))
                .getIndex();
            if (index.isPresent()) {
                toBeORed.add(index.get());
            }
        }
        BM r = bitmaps.create();
        bitmaps.or(r, toBeORed);
        return r;
    }

    private <BM> MinMaxPriorityQueue<MiruTermCount> rankContributors(MiruBitmaps<BM> bitmaps,
        final MiruRequest<RecoQuery> request,
        MiruRequestContext<BM> requestContext,
        BM join1)
        throws Exception {

        int fieldId = requestContext.getSchema().getFieldId(request.query.aggregateFieldName2);
        log.debug("rankContributors: fieldId={}", fieldId);

        final MinMaxPriorityQueue<MiruTermCount> userHeap = MinMaxPriorityQueue.orderedBy(new Comparator<MiruTermCount>() {

            @Override
            public int compare(MiruTermCount o1, MiruTermCount o2) {
                return -Long.compare(o1.count, o2.count); // minus to reverse :)
            }
        }).maximumSize(request.query.desiredNumberOfDistincts).create(); // overloaded :(

        aggregateUtil.stream(bitmaps, request.tenantId, requestContext, join1, Optional.<BM>absent(), fieldId, request.query.retrieveFieldName2,
            new CallbackStream<MiruTermCount>() {
                @Override
                public MiruTermCount callback(MiruTermCount v) throws Exception {
                    if (v != null) {
                        userHeap.add(v);
                    }
                    return v;
                }
            });
        return userHeap;
    }

    private <BM> BM possibleContributors(MiruBitmaps<BM> bitmaps, MiruRequestContext<BM> requestContext, final MiruRequest<RecoQuery> request, BM answer)
        throws Exception {

        int fieldId = requestContext.getSchema().getFieldId(request.query.aggregateFieldName1);
        MiruFieldIndex<BM> fieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.pairedLatest);
        List<BM> toBeORed = new ArrayList<>();
        MiruIntIterator answerIterator = bitmaps.intIterator(answer);
        log.debug("possibleContributors: fieldId={}", fieldId);
        // feeds us our docIds
        while (answerIterator.hasNext()) {
            int id = answerIterator.next();
            MiruTermId[] fieldValues = requestContext.getActivityIndex().get(request.tenantId, id, fieldId);
            log.trace("possibleContributors: fieldValues={}", (Object) fieldValues);
            if (fieldValues != null && fieldValues.length > 0) {
                Optional<BM> index = fieldIndex.get(
                    fieldId,
                    indexUtil.makePairedLatestTerm(fieldValues[0], request.query.aggregateFieldName2))
                    .getIndex();
                if (index.isPresent()) {
                    toBeORed.add(index.get());
                }
            }
        }
        BM r = bitmaps.create();
        log.debug("possibleContributors: toBeORed.size={}", toBeORed.size());
        bitmaps.or(r, toBeORed);
        log.trace("possibleContributors: r={}", r);
        return r;
    }

    private <BM> RecoAnswer score(MiruBitmaps<BM> bitmaps, final MiruRequest<RecoQuery> request, BM join2, MiruRequestContext<BM> requestContext,
        final BloomIndex<BM> bloomIndex, final List<BloomIndex.Mights<MiruTermCount>> wantBits) throws Exception {

        final int fieldId = requestContext.getSchema().getFieldId(request.query.aggregateFieldName3);
        log.debug("score: fieldId={}", fieldId);

        final MiruFieldIndex<BM> bloomFieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.bloom);
        final MinMaxPriorityQueue<MiruTermCount> heap = MinMaxPriorityQueue.orderedBy(new Comparator<MiruTermCount>() {

            @Override
            public int compare(MiruTermCount o1, MiruTermCount o2) {
                return -Long.compare(o1.count, o2.count); // minus to reverse :)
            }
        }).maximumSize(request.query.desiredNumberOfDistincts).create();

        // feeds us all recommended parents <field3>
        aggregateUtil.stream(bitmaps, request.tenantId, requestContext, join2, Optional.<BM>absent(), fieldId, request.query.retrieveFieldName3,
            new CallbackStream<MiruTermCount>() {
                @Override
                public MiruTermCount callback(MiruTermCount v) throws Exception {
                    if (v != null) {
                        MiruTermId[] fieldValues = v.mostRecent;
                        log.trace("score.fieldValues={}", (Object) fieldValues);
                        if (fieldValues != null && fieldValues.length > 0) {
                            Optional<BM> index = bloomFieldIndex.get(
                                fieldId,
                                indexUtil.makeBloomTerm(fieldValues[0], request.query.retrieveFieldName2))
                                .getIndex();
                                final MutableInt count = new MutableInt(0);
                            if (index.isPresent()) {
                                bloomIndex.mightContain(index.get(), wantBits, new BloomIndex.MightContain<MiruTermCount>() {

                                    @Override
                                    public void mightContain(MiruTermCount value) {
                                        count.add(value.count);
                                    }
                                });
                                heap.add(new MiruTermCount(fieldValues[0], null, count.longValue()));

                                for (BloomIndex.Mights<MiruTermCount> boo : wantBits) {
                                    boo.reset();
                                }
                            }
                        }
                    }
                    return v;
                }
            });

        List<Recommendation> results = new ArrayList<>();
        for (MiruTermCount result : heap) {
            results.add(new Recommendation(result.termId, result.count));
        }
        log.debug("score: results.size={}", results.size());
        return new RecoAnswer(results, 1);
    }

}
