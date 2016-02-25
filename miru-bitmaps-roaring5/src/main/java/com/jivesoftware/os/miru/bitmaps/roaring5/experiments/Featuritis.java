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

        int precision = 64;


        Random r = new Random(1234);

        for (int c = 0; c < 100; c++) {
            RoaringBitmap[] bits = new RoaringBitmap[precision];
            for (int i = 0; i < precision; i++) {
                bits[i] = new RoaringBitmap();
            }

            int count = 10000;
            int cardinality = 3000;
            Set<Integer> desired = new HashSet<>();
            for (int i = 0; i < count; i++) {
                int term = r.nextInt(cardinality) + 1;
                desired.add(term);
                 int bit = 0;
                    while (term != 0) {
                    if ((term & 1) != 0) {
                        bits[bit].add(i);
                    }
                    bit++;
                    term >>>= 1;
                }
            }

            RoaringBitmap input = new RoaringBitmap();
            input.add(0, count);

            int[] solution = new int[1];
            long start = System.currentTimeMillis();
            solve(bits, input, (output, cardinality1) -> {
                solution[0]++;
                //int term = read(output);
                //System.out.println(term + " count=" + cardinality1);
                return true;
            });
            System.out.println("elapse:" + (System.currentTimeMillis() - start) + " millis  found:" + solution[0] + " expected:" + desired.size());
            System.out.println("-------------------------------------------");
        }

    }

    static private int read(boolean[] bools) {
        int term = 0;
        for (int j = 0; j < bools.length; j++) {
            if (bools[j]) {
                term |= (1 << j);
            }
        }
        return term;
    }

    static private void solve(RoaringBitmap[] bits, RoaringBitmap candidate, Solution solution) {
        int bitIndex = bits.length - 1;
        while (bitIndex > -1) {
            RoaringBitmap ones = RoaringBitmap.and(candidate, bits[bitIndex]);
            if (!ones.isEmpty()) {
                break;
            }
            bitIndex--;
        }
        if (bitIndex > -1) {
            solve(bits, 0, bitIndex, candidate, new boolean[bits.length], solution);
        }
    }

    static void solve(RoaringBitmap[] bits, int depth, int bitIndex, RoaringBitmap candidate, boolean[] output, Solution solution) {

        RoaringBitmap zero = RoaringBitmap.andNot(candidate, bits[bitIndex]);
        RoaringBitmap ones = RoaringBitmap.and(candidate, bits[bitIndex]);
        if (bitIndex > 0) {
            output[bitIndex] = true;
            solve(bits, depth + 1, bitIndex - 1, ones, output, solution);
        } else if (!ones.isEmpty()) {
            output[bitIndex] = true;
            solution.stream(output, ones.getCardinality());
        }
        if (bitIndex > 0) {
            output[bitIndex] = false;
            solve(bits, depth + 1, bitIndex - 1, zero, output, solution);
        } else if (!zero.isEmpty()) {
            output[bitIndex] = false;
            solution.stream(output, zero.getCardinality());
        }
    }

    interface Solution {

        boolean stream(boolean[] output, int cardinality);
    }

}
