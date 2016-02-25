package com.jivesoftware.os.miru.bitmaps.roaring5.experiments;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.roaringbitmap.RoaringBitmap;

/**
 *
 * @author jonathan.colt
 */
public class Featuritis {

    public static void main(String[] args) {

        Random r = new Random(1234);

        for (int c = 0; c < 100; c++) {
            int precision = 64;
            Featuritis fi = new Featuritis(precision);

            int count = 10000;
            int cardinality = 3000;
            Set<Integer> desired = new HashSet<>();
            for (int i = 0; i < count; i++) {
                int t = r.nextInt(cardinality) + 1;
                desired.add(t);
                fi.add(t, i);
            }

            RoaringBitmap input = new RoaringBitmap();
            input.add(0, count);

            int[] solution = new int[1];
            long start = System.currentTimeMillis();
            fi.solve(input, (output, cardinality1) -> {
                solution[0]++;
                int term = fi.read(output);
                System.out.println(term + " count=" + cardinality1);
                return true;
            });
            System.out.println("elapse:" + (System.currentTimeMillis() - start) + " millis  found:" + solution[0] + " expected:" + desired.size());
            System.out.println("-------------------------------------------");
        }

    }

    int precision;
    RoaringBitmap[] bits;

    public Featuritis(int precision) {
        this.precision = precision;
        this.bits = new RoaringBitmap[precision];
        for (int i = 0; i < precision; i++) {
            this.bits[i] = new RoaringBitmap();
        }
    }

    public void add(int term, int aid) {
        int bit = 0;
        while (term != 0) {
            if ((term & 1) != 0) {
                bits[bit].add(aid);
            }
            bit++;
            term >>>= 1;
        }
    }

    public int read(boolean[] bools) {
        int term = 0;
        for (int j = 0; j < precision; j++) {
            if (bools[j]) {
                term |= (1 << j);
            }
        }
        return term;
    }

    private void solve(RoaringBitmap candidate, Solution solution) {
        int bitIndex = precision - 1;
        while (bitIndex > -1) {
            RoaringBitmap ones = RoaringBitmap.and(candidate, bits[bitIndex]);
            if (!ones.isEmpty()) {
                break;
            }
            bitIndex--;
        }
        if (bitIndex > -1) {
            solve(0, bitIndex, candidate, new boolean[precision], solution);
        }
    }

    private void solve(int depth, int bitIndex, RoaringBitmap candidate, boolean[] output, Solution solution) {

        RoaringBitmap zero = RoaringBitmap.andNot(candidate, bits[bitIndex]);
        RoaringBitmap ones = RoaringBitmap.and(candidate, bits[bitIndex]);
        if (bitIndex > 0) {
            output[bitIndex] = true;
            solve(depth + 1, bitIndex - 1, ones, output, solution);
        } else if (!ones.isEmpty()) {
            output[bitIndex] = true;
            solution.stream(output, ones.getCardinality());
        }
        if (bitIndex > 0) {
            output[bitIndex] = false;
            solve(depth + 1, bitIndex - 1, zero, output, solution);
        } else if (!zero.isEmpty()) {
            output[bitIndex] = false;
            solution.stream(output, zero.getCardinality());
        }
    }

    interface Solution {

        boolean stream(boolean[] output, int cardinality);
    }

}
