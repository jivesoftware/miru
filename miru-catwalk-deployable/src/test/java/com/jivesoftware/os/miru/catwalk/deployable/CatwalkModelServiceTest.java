package com.jivesoftware.os.miru.catwalk.deployable;

import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.catwalk.deployable.CatwalkModelService.FeatureRange;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery.CatwalkFeature;
import com.jivesoftware.os.miru.catwalk.shared.FeatureScore;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 */
public class CatwalkModelServiceTest {

    @Test
    public void testKeySerDer() throws Exception {
        int fromPartitionId = 123;
        int toPartitionId = Integer.MAX_VALUE - 456;
        String featureName = "test";
        CatwalkFeature[] features = { new CatwalkFeature(featureName, null, null, 1f) };
        byte[] keyBytes = CatwalkModelService.modelPartitionKey("catwalkId",
            "modelId",
            featureName,
            fromPartitionId,
            toPartitionId);

        FeatureRange featureRange = CatwalkModelService.getFeatureRange(keyBytes, features);
        assertEquals(featureRange.featureName, featureName);
        assertEquals(featureRange.fromPartitionId, fromPartitionId);
        assertEquals(featureRange.toPartitionId, toPartitionId);
    }

    @Test
    public void testValueSerDer() throws Exception {
        boolean partitionIsClosed = false;
        List<FeatureScore> featureScores = Arrays.asList(new FeatureScore(terms("term1", "term2", "term3"), new long[] { 1 }, 3, 1),
            new FeatureScore(terms("term4", "term5", "term6"), new long[] { 2 }, 4, 1),
            new FeatureScore(terms("term7", "term8", "term9"), new long[] { 3 }, 5, 1));
        MiruTimeRange timeRange = new MiruTimeRange(123L, Long.MAX_VALUE - 456L);
        byte[] valueBytes = CatwalkModelService.valueToBytes(partitionIsClosed,
            6,
            12,
            1,
            featureScores,
            timeRange);

        ModelFeatureScores modelFeatureScores = CatwalkModelService.valueFromBytes(valueBytes, 0);

        assertEquals(modelFeatureScores.partitionIsClosed, partitionIsClosed);
        assertEquals(modelFeatureScores.featureScores.size(), featureScores.size());
        for (int i = 0; i < featureScores.size(); i++) {
            FeatureScore actual = modelFeatureScores.featureScores.get(i);
            FeatureScore expected = featureScores.get(i);
            assertEquals(actual.termIds, expected.termIds);
            assertEquals(actual.numerators, expected.numerators);
            assertEquals(actual.denominator, expected.denominator);
        }
        assertEquals(modelFeatureScores.timeRange, timeRange);
    }

    private static MiruTermId[] terms(String... terms) {
        MiruTermId[] termIds = new MiruTermId[terms.length];
        for (int i = 0; i < terms.length; i++) {
            termIds[i] = new MiruTermId(terms[i].getBytes());
        }
        return termIds;
    }
}
