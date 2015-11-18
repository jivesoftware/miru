/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.jivesoftware.os.miru.bitmaps.ewah;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah.FastAggregation;
import java.util.Random;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/** @author jonathan */
public class AnswerCardinalityLastSetBitmapStorageNGTest {

    @Test
    public void testLastSet() {
        Random random = new Random(1_234);
        for (int maxSize = 10; maxSize < 1_000_000; maxSize *= 2) {
            for (int i = 0; i < 10; i++) {
                int lastSetA = maxSize + random.nextInt(3);
                EWAHCompressedBitmap a = randoBagoBitso(random, maxSize, lastSetA);

                int lastSetB = maxSize + random.nextInt(100);
                EWAHCompressedBitmap b = randoBagoBitso(random, maxSize, lastSetB);

                EWAHCompressedBitmap result = new EWAHCompressedBitmap();
                AnswerCardinalityLastSetBitmapStorage answerCollector = new AnswerCardinalityLastSetBitmapStorage(result);
                a.orToContainer(b, answerCollector);
                assertEquals(Math.max(lastSetA, lastSetB), answerCollector.getLastSetBit());

                result = new EWAHCompressedBitmap();
                answerCollector = new AnswerCardinalityLastSetBitmapStorage(result);
                FastAggregation.bufferedorWithContainer(answerCollector, 1_024, a, b);

            }
        }
    }

    EWAHCompressedBitmap randoBagoBitso(Random random, int count, int lastSetBit) {
        int set = 0;
        int maxRun = lastSetBit / count;
        EWAHCompressedBitmap a = new EWAHCompressedBitmap();
        for (int i = 0; i < count; i++) {
            set += 1 + random.nextInt(maxRun);
            a.set(set);
        }
        a.set(lastSetBit);
        return a;

    }

}
