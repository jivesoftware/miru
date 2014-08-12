package com.jivesoftware.os.miru.service.stream.filter;

import com.googlecode.javaewah.BitmapStorage;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import java.nio.LongBuffer;

/**
 *
 * @author jonathan
 */
public class AnswerCardinalityLastSetBitmapStorage implements BitmapStorage {

    private static final int oneLessBit = EWAHCompressedBitmap.wordinbits - 1;
    private final EWAHCompressedBitmap container;

    private int oneBits;
    private int lastSetBit = -1;
    private int position;

    public AnswerCardinalityLastSetBitmapStorage(EWAHCompressedBitmap container) {
        this.container = container;
    }

    public int getCount() {
        return oneBits;
    }

    public int getLastSetBit() {
        return lastSetBit;
    }

    @Override
    public void addWord(long word) {
        if (word != 0) {
            oneBits += Long.bitCount(word);
            lastSetBit = position + (oneLessBit - Long.numberOfLeadingZeros(word));
        }
        position += EWAHCompressedBitmap.wordinbits;
        container.addWord(word);
    }

    @Override
    public void addStreamOfLiteralWords(LongBuffer words, int start, int number) {
        int lastIndexWithBit = -1;
        for (int i = start; i < start + number; i++) {
            long d = words.get(i);
            if (d != 0) {
                oneBits += Long.bitCount(d);
                lastIndexWithBit = i;
            }
        }

        if (lastIndexWithBit > -1) {
            int offestInWords = (lastIndexWithBit - start);
            position +=  offestInWords * EWAHCompressedBitmap.wordinbits;
            lastSetBit = position + (oneLessBit - Long.numberOfLeadingZeros(words.get(lastIndexWithBit)));
            position += EWAHCompressedBitmap.wordinbits * (number - offestInWords);
        } else {
            position += EWAHCompressedBitmap.wordinbits * number;
        }
        container.addStreamOfLiteralWords(words, start, number);
    }


    @Override
    public void addStreamOfEmptyWords(boolean v, long number) {
        if (number > 0) {
            long numberOfBits = EWAHCompressedBitmap.wordinbits * number;
            position += numberOfBits;
            if (v) {
                oneBits += numberOfBits;
                lastSetBit = position - 1;
            }
        }
        container.addStreamOfEmptyWords(v, number);
    }

    @Override
    public void addStreamOfNegatedLiteralWords(LongBuffer words, int start, int number) {
        int lastIndexWithBit = -1;
        for (int i = start; i < start + number; i++) {
            long d = ~words.get(i);
            if (d != 0) {
                oneBits += Long.bitCount(d);
                lastIndexWithBit = i;
            }
        }

        if (lastIndexWithBit > -1) {
            int offestInWords = (lastIndexWithBit - start);
            position +=  offestInWords * EWAHCompressedBitmap.wordinbits;
            lastSetBit = position + (oneLessBit - Long.numberOfLeadingZeros(~words.get(lastIndexWithBit)));
            position += EWAHCompressedBitmap.wordinbits * (number - offestInWords);
        } else {
            position += EWAHCompressedBitmap.wordinbits * number;
        }

        container.addStreamOfNegatedLiteralWords(words, start, number);
    }

    @Override
    public void setSizeInBits(int bits) {
        container.setSizeInBits(bits);
    }

}
