package com.jivesoftware.os.miru.plugin.solution;

import com.beust.jcommander.internal.Sets;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.Arrays;
import java.util.Set;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 */
public class MiruAggregateUtilTest {

    @Test
    public void testPermutations() throws Exception {
        MiruAggregateUtil util = new MiruAggregateUtil();
        MiruTermId[] level1 = {
            new MiruTermId("a".getBytes()),
            new MiruTermId("b".getBytes()),
            new MiruTermId("c".getBytes())
        };
        MiruTermId[] level2 = {
            new MiruTermId("x".getBytes()),
            new MiruTermId("y".getBytes())
        };
        MiruTermId[] level3 = {
            new MiruTermId("1".getBytes()),
            new MiruTermId("2".getBytes()),
            new MiruTermId("3".getBytes()),
            new MiruTermId("4".getBytes())
        };
        MiruTermId[][] termIds = {
            level1,
            level2,
            level3
        };
        Set<Feature> features = Sets.newHashSet();
        util.permutate(0, 3, termIds, (index, termIds1) -> {
            features.add(new Feature(termIds1));
            return true;
        });
        assertEquals(features.size(), level1.length * level2.length * level3.length);
    }

    private static class Feature {
        final MiruTermId[] termIds;

        public Feature(MiruTermId[] termIds) {
            this.termIds = termIds;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Feature feature = (Feature) o;

            return Arrays.deepEquals(termIds, feature.termIds);

        }

        @Override
        public int hashCode() {
            return termIds != null ? Arrays.hashCode(termIds) : 0;
        }
    }
}