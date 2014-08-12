package com.jivesoftware.os.miru.test;

import java.util.Random;

/**
 * Generates random numbers with two-tailed distribution by composing a pair of one-tailed generators
 * (see {@link com.jivesoftware.os.miru.test.OneTailedRandomNumber}), using the given
 * lower, middle, and upper values with their respective weights and granularity.
 */
public class TwoTailedRandomNumber implements WeightedRandomNumber {

    private final long middle;
    private final OneTailedRandomNumber lowerNumber;
    private final OneTailedRandomNumber upperNumber;

    public TwoTailedRandomNumber(long lower, long middle, long upper, double lowerWeight, double upperWeight, int lowerGranularity, int upperGranularity) {
        this.middle = middle;
        this.lowerNumber = new OneTailedRandomNumber(lowerWeight, lowerGranularity, 0, middle - lower);
        this.upperNumber = new OneTailedRandomNumber(upperWeight, upperGranularity, 0, upper - middle);
    }

    @Override
    public long get(Random random) {
        return middle - lowerNumber.get(random) + upperNumber.get(random);
    }
}
