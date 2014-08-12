package com.jivesoftware.os.miru.test;

import java.util.Random;

/**
 * Generates random numbers with long-tailed distribution. Generated numbers fall between the given lower (inclusive)
 * and upper (exclusive) bounds, weighted towards the minimum such that each distinct value is <code>weight</code>
 * times more likely to occur than the subsequent value. The range is segmented according to the desired granularity
 * (the number of distinct segments to weight across the desired range). Within a segment, values have random uniform
 * distribution (equal weight).
 * <p/>
 * If <code>granularity == upper - lower</code> then every number in the range is a distinct segment.
 * <p/>
 * For example: weight=10, granularity=5, min=0, max=5 results in [0] 90%, [1] 9%, [2] 0.9%, [3], 0.09%, [4] 0.01%
 * <p/>
 * Cost is linear with respect to granularity.
 */
public class OneTailedRandomNumber implements WeightedRandomNumber {

    private final double[] probability;
    private final long lower;
    private final long segmentSize;

    public OneTailedRandomNumber(double weight, int granularity, long lower, long upper) {
        final long range = upper - lower;

        this.lower = lower;
        this.segmentSize = range / granularity;

        double[] probability = new double[granularity];
        probability[0] = 1.0;
        double pSum = probability[0];
        for (int i = 1; i < granularity; i++) {
            probability[i] = probability[i - 1] / weight;
            pSum += probability[i];
        }
        for (int i = 0; i < granularity; i++) {
            probability[i] /= pSum;
        }
        this.probability = probability;
    }

    @Override
    public long get(Random random) {
        final double r = (double) Math.abs(random.nextInt()) / Integer.MAX_VALUE;
        double pSeek = 0;
        for (int i = 0; i < probability.length; i++) {
            pSeek += probability[i];
            if (i == probability.length - 1 || r < pSeek) {
                return lower + i * segmentSize + (long) (random.nextDouble() * segmentSize);
            }
        }
        throw new RuntimeException("unreachable");
    }

    // uncomment and run this to try some weight/range combinations
    /*
    public static void main(String[] args) {
        final Random random = new Random();
        final double weight = 2;
        final int granularity = 5;
        final long upper = 5;

        OneTailedRandomNumber oneTailedRandomNumber = new OneTailedRandomNumber(weight, granularity, 0, upper);
        int[] buckets = new int[granularity];
        for (int i = 0; i < 1_000_000; i++) {
            buckets[(int) (oneTailedRandomNumber.get(random) * granularity / upper)]++;
        }
        for (int i = 0; i < buckets.length; i++) {
            System.out.println("[" + (i * upper / granularity) + "] " + buckets[i]);
        }
    }
    */
}
