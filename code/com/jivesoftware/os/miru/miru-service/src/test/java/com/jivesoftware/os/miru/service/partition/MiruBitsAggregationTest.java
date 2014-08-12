/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.miru.service.partition;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah.FastAggregation;
import java.util.Arrays;
import org.apache.commons.lang.ArrayUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.service.util.BitsTestUtil.ewahBits;
import static org.testng.AssertJUnit.assertEquals;

/** @author jonathan */
public class MiruBitsAggregationTest {

    EWAHCompressedBitmap[] bitmaps;
    int size = 72;

    EWAHCompressedBitmap allOnes;
    EWAHCompressedBitmap only_0_4_8;
    EWAHCompressedBitmap only_1_5_9;
    EWAHCompressedBitmap only_2_6_10;
    EWAHCompressedBitmap only_3_7_11;

    @BeforeMethod
    public void setUpMethod() throws Exception {
        EWAHCompressedBitmap a = ewahBits(0, size, 1, 3);
        EWAHCompressedBitmap b = ewahBits(0, size, 2);
        bitmaps = new EWAHCompressedBitmap[] { a, b };

        allOnes = ewahBits(0, size, 1);
        only_0_4_8 = ewahBits(0, size, 4);
        only_1_5_9 = ewahBits(1, size, 4);
        only_2_6_10 = ewahBits(2, size, 4);
        only_3_7_11 = ewahBits(3, size, 4);
    }

    @Test
    public void testAnd() {
        EWAHCompressedBitmap r = new EWAHCompressedBitmap();
        FastAggregation.bufferedandWithContainer(r, 10, bitmaps);
        EWAHCompressedBitmap e = new EWAHCompressedBitmap();
        int i = 0;
        while (i < size) {
            e.set(i);
            i++;
            i++;
            i++;
            i++;
        }
        Assert.assertTrue(r.xor(e).cardinality() == 0);
    }

    @Test
    public void testAndPQ() {
        EWAHCompressedBitmap r = new EWAHCompressedBitmap();
        MiruBitsAggregation.and(r, bitmaps);
        EWAHCompressedBitmap e = new EWAHCompressedBitmap();
        int i = 0;
        while (i < size) {
            e.set(i);
            i++;
            i++;
            i++;
            i++;
        }
        Assert.assertTrue(r.xor(e).cardinality() == 0);
    }

    @Test
    public void testPButNotQ() {
        EWAHCompressedBitmap r = new EWAHCompressedBitmap();
        MiruBitsAggregation.pButNotQ(r, bitmaps);
        EWAHCompressedBitmap e = new EWAHCompressedBitmap();
        int i = 0;
        while (i < size) {
            i++;
            e.set(i);
            i++;
            i++;
            i++;
        }
        System.out.println("E:" + Arrays.toString(e.toArray()));
        System.out.println("R:" + Arrays.toString(r.toArray()));
        Assert.assertTrue(r.xor(e).cardinality() == 0);

        EWAHCompressedBitmap expected = bitmaps[0];
        for (i = 1; i < bitmaps.length; i++) {
            expected = expected.andNot(bitmaps[i]);
        }
        Assert.assertEquals(r, expected);
    }

    @Test
    public void testPButNotQReversed() {
        EWAHCompressedBitmap[] bitmaps = Arrays.copyOf(this.bitmaps, this.bitmaps.length);
        ArrayUtils.reverse(bitmaps);

        for (int i = 0; i < bitmaps.length; i++) {
            bitmaps[i] = this.bitmaps[this.bitmaps.length - 1 - i];
        }

        EWAHCompressedBitmap r = new EWAHCompressedBitmap();
        MiruBitsAggregation.pButNotQ(r, bitmaps);
        EWAHCompressedBitmap e = new EWAHCompressedBitmap();
        int i = 0;
        while (i < size) {
            i++;
            i++;
            e.set(i);
            i++;
            i++;
        }
        System.out.println("E:" + Arrays.toString(e.toArray()));
        System.out.println("R:" + Arrays.toString(r.toArray()));
        Assert.assertTrue(r.xor(e).cardinality() == 0);

        EWAHCompressedBitmap expected = bitmaps[0];
        for (i = 1; i < bitmaps.length; i++) {
            expected = expected.andNot(bitmaps[i]);
        }
        Assert.assertEquals(r, expected);
    }

    @Test
    public void testPButNotQMulti() {
        EWAHCompressedBitmap r = new EWAHCompressedBitmap();
        MiruBitsAggregation.pButNotQ(r, new EWAHCompressedBitmap[] { allOnes, only_0_4_8, only_1_5_9, only_2_6_10 });

        EWAHCompressedBitmap e = only_3_7_11;

        System.out.println("E:" + Arrays.toString(e.toArray()));
        System.out.println("R:" + Arrays.toString(r.toArray()));
        Assert.assertTrue(r.xor(e).cardinality() == 0);
    }

    @Test
    public void testPButNotQMultiReversed() {
        EWAHCompressedBitmap r = new EWAHCompressedBitmap();
        MiruBitsAggregation.pButNotQ(r, new EWAHCompressedBitmap[] { allOnes, only_3_7_11, only_2_6_10, only_1_5_9 });

        EWAHCompressedBitmap e = only_0_4_8;

        System.out.println("E:" + Arrays.toString(e.toArray()));
        System.out.println("R:" + Arrays.toString(r.toArray()));
        Assert.assertTrue(r.xor(e).cardinality() == 0);
    }

    @Test
    public void testOr() {
        EWAHCompressedBitmap r = new EWAHCompressedBitmap();
        FastAggregation.bufferedorWithContainer(r, 10, bitmaps);
        EWAHCompressedBitmap e = new EWAHCompressedBitmap();
        int i = 0;
        while (i < size) {
            e.set(i);
            i++;
            e.set(i);
            i++;
            e.set(i);
            i++;
            i++;
        }
        Assert.assertTrue(r.xor(e).cardinality() == 0);

    }

    @Test
    public void testXOr() {
        EWAHCompressedBitmap r = new EWAHCompressedBitmap();
        FastAggregation.bufferedxorWithContainer(r, 10, bitmaps);
        EWAHCompressedBitmap e = new EWAHCompressedBitmap();
        int i = 0;
        while (i < size) {
            i++;
            e.set(i);
            i++;
            e.set(i);
            i++;
            i++;
        }
        Assert.assertTrue(r.xor(e).cardinality() == 0);
    }

    @Test
    public void testBitStuff() {

        test("AND", ~0, 0, 0, 0, new BF() {
            @Override
            public long bf(long P, long Q) {
                return (P & Q);
            }
        });

        test("NAND", 0, ~0, ~0, ~0, new BF() {
            @Override
            public long bf(long P, long Q) {
                return ~(P & Q);
            }
        });

        test("OR", ~0, ~0, ~0, 0, new BF() {
            @Override
            public long bf(long P, long Q) {
                return (P | Q);
            }
        });

        test("NOR", 0, 0, 0, ~0, new BF() {
            @Override
            public long bf(long P, long Q) {
                return ~(P | Q);
            }
        });

        test("XOR", 0, ~0, ~0, 0, new BF() {
            @Override
            public long bf(long P, long Q) {
                return (P ^ Q);
            }
        });

        test("XNOR", ~0, 0, 0, ~0, new BF() {
            @Override
            public long bf(long P, long Q) {
                return ~(P ^ Q);
            }
        });

        test("PButNotQ", 0, ~0, 0, 0, new BF() {
            @Override
            public long bf(long P, long Q) {
                return (P & (~Q));
            }
        });
        test("PButNotQ", 0, ~0, 0, 0, new BF() {
            @Override
            public long bf(long P, long Q) {
                return ~((~P) | Q);
            }
        });

        test("IfThen", ~0, 0, ~0, ~0, new BF() {
            @Override
            public long bf(long P, long Q) {
                return ~(P & (~Q));
            }
        });
        test("IfThen", ~0, 0, ~0, ~0, new BF() {
            @Override
            public long bf(long P, long Q) {
                return ((~P) | Q);
            }
        });

        test("ThenIf", ~0, ~0, 0, ~0, new BF() {
            @Override
            public long bf(long P, long Q) {
                return ~((~P) & Q);
            }
        });
        test("ThenIf", ~0, ~0, 0, ~0, new BF() {
            @Override
            public long bf(long P, long Q) {
                return (P | (~Q));
            }
        });

        test("NotPButQ", 0, 0, ~0, 0, new BF() {
            @Override
            public long bf(long P, long Q) {
                return ((~P) & Q);
            }
        });
        test("NotPButQ", 0, 0, ~0, 0, new BF() {
            @Override
            public long bf(long P, long Q) {
                return ~(P | (~Q));
            }
        });

    }

    void test(String functionName, long tt, long tf, long ft, long ff, BF bf) {
        assertEquals("T " + functionName + " T expect " + ((tt == 0) ? false : true), tt, bf.bf(~0, ~0));
        assertEquals("T " + functionName + " F expect " + ((tf == 0) ? false : true), tf, bf.bf(~0, 0));
        assertEquals("F " + functionName + " T expect " + ((ft == 0) ? false : true), ft, bf.bf(0, ~0));
        assertEquals("F " + functionName + " F expect " + ((ff == 0) ? false : true), ff, bf.bf(0, 0));
        System.out.println(functionName + " Passed");
    }

    static interface BF {

        long bf(long P, long Q);
    }

}
