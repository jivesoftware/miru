package com.jivesoftware.os.miru.service.stream;

import com.google.common.collect.Maps;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import org.testng.annotations.Test;
import org.xerial.snappy.SnappyOutputStream;

import static org.testng.Assert.assertTrue;

/**
 *
 */
public class MiruIndexValueBitsTest {

    @Test
    public void testComputeDifference() {
        TIntList all = new TIntArrayList(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 });
        TIntList set = new TIntArrayList(new int[] { 1, 3, 5, 7, 9 });
        TIntList diff = MiruIndexValueBits.computeDifference(all, set);

        int[] found = diff.toArray();
        int[] expected = { 0, 2, 4, 6, 8, 10, 11 };
        assertTrue(Arrays.equals(found, expected), "Not equal: " + Arrays.toString(found) + " != " + Arrays.toString(expected));
    }

    @Test(enabled = false)
    public void testFeatureBurn() throws Exception {
        //PatriciaTrie<Float> model = new PatriciaTrie<>();
        HashMap<String, Float> model = Maps.newHashMap();

        String[] featureTypes = { "activityType context", "activityType user", "context user" };

        int userCardinality = 10_000;
        int activityTypeCardinality = 10;
        int contextCardinality = 5_000;

        Random r = new Random(1234);
        int numFeatures = 100_000;
        for (int i = 0; i < numFeatures; i++) {
            String type = featureTypes[r.nextInt(featureTypes.length)];
            if (type.equals("activityType context")) {
                model.put(type + ":" + r.nextInt(activityTypeCardinality) + ":600 " + r.nextInt(contextCardinality), r.nextFloat());
            } else if (type.equals("activityType user")) {
                model.put(type + ":" + r.nextInt(activityTypeCardinality) + ":3 " + r.nextInt(userCardinality), r.nextFloat());
            } else if (type.equals("context user")) {
                model.put(type + ":600 " + r.nextInt(contextCardinality) + ":3 " + r.nextInt(userCardinality), r.nextFloat());
            }
        }

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bytes);
        out.writeObject(model);
        System.out.println("size: " + bytes.size());

        bytes = new ByteArrayOutputStream();
        SnappyOutputStream snappify = new SnappyOutputStream(bytes);
        out = new ObjectOutputStream(snappify);
        out.writeObject(model);
        System.out.println("snappy: " + bytes.size());

        long start = System.currentTimeMillis();
        float cumulativeScore = 0f;
        int cumulativeHits = 0;
        long bytesMoved = 0;
        for (int i = 0; i < 1_000_000_000; i++) {
            //Set<String> features = Sets.newHashSet();
            int context = r.nextInt(contextCardinality);
            int hits = 0;
            float score = 0f;

            bytesMoved += "102 1238283".length();

            for (int j = 0; j < 10; j++) {
                String feature1 = "activityType context" + ":" + r.nextInt(activityTypeCardinality) + ":600 " + context;
                String feature2 = "activityType user" + ":" + r.nextInt(activityTypeCardinality) + ":3 " + r.nextInt(userCardinality);
                String feature3 = "context user" + ":600 " + context + ":3 " + r.nextInt(userCardinality);

                bytesMoved += feature1.length() + feature2.length() + feature3.length();

                Float score1 = model.get(feature1);
                Float score2 = model.get(feature2);
                Float score3 = model.get(feature3);
                if (score1 != null) {
                    hits++;
                    score += score1;
                }
                if (score2 != null) {
                    hits++;
                    score += score2;
                }
                if (score3 != null) {
                    hits++;
                    score += score3;
                }
            }
            score /= hits;
            cumulativeScore = score * 0.01f + cumulativeScore * 0.99f;
            cumulativeHits += hits;

            if (i % 10_000 == 0) {
                int cumulativeMisses = (10 * 3 * i) - cumulativeHits;
                System.out.println("10_000 in " + (System.currentTimeMillis() - start) + " ms -"
                    + " hits: " + cumulativeHits
                    + " misses: " + cumulativeMisses
                    + " bytes:" + bytesMoved);
                start = System.currentTimeMillis();
                bytesMoved = 0;
            }
        }
    }

}
