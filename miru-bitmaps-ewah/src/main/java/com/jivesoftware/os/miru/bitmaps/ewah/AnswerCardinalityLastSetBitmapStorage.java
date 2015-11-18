package com.jivesoftware.os.miru.bitmaps.ewah;

import com.googlecode.javaewah.BitmapStorage;
import com.googlecode.javaewah.EWAHCompressedBitmap;

/**
 * @author jonathan
 */
public class AnswerCardinalityLastSetBitmapStorage implements BitmapStorage {

    private static final int oneLessBit = EWAHCompressedBitmap.WORD_IN_BITS - 1;
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
        position += EWAHCompressedBitmap.WORD_IN_BITS;
        container.addWord(word);
    }

    @Override
    public void addStreamOfLiteralWords(long[] words, int start, int number) {
        int lastIndexWithBit = -1;
        for (int i = start; i < start + number; i++) {
            long d = words[i];
            if (d != 0) {
                oneBits += Long.bitCount(d);
                lastIndexWithBit = i;
            }
        }

        if (lastIndexWithBit > -1) {
            int offestInWords = (lastIndexWithBit - start);
            position += offestInWords * EWAHCompressedBitmap.WORD_IN_BITS;
            lastSetBit = position + (oneLessBit - Long.numberOfLeadingZeros(words[lastIndexWithBit]));
            position += EWAHCompressedBitmap.WORD_IN_BITS * (number - offestInWords);
        } else {
            position += EWAHCompressedBitmap.WORD_IN_BITS * number;
        }
        container.addStreamOfLiteralWords(words, start, number);
    }


    @Override
    public void addStreamOfEmptyWords(boolean v, long number) {
        if (number > 0) {
            long numberOfBits = EWAHCompressedBitmap.WORD_IN_BITS * number;
            position += numberOfBits;
            if (v) {
                oneBits += numberOfBits;
                lastSetBit = position - 1;
            }
        }
        container.addStreamOfEmptyWords(v, number);
    }

    @Override
    public void addStreamOfNegatedLiteralWords(long[] words, int start, int number) {
        int lastIndexWithBit = -1;
        for (int i = start; i < start + number; i++) {
            long d = ~words[i];
            if (d != 0) {
                oneBits += Long.bitCount(d);
                lastIndexWithBit = i;
            }
        }

        if (lastIndexWithBit > -1) {
            int offestInWords = (lastIndexWithBit - start);
            position += offestInWords * EWAHCompressedBitmap.WORD_IN_BITS;
            lastSetBit = position + (oneLessBit - Long.numberOfLeadingZeros(~words[lastIndexWithBit]));
            position += EWAHCompressedBitmap.WORD_IN_BITS * (number - offestInWords);
        } else {
            position += EWAHCompressedBitmap.WORD_IN_BITS * number;
        }

        container.addStreamOfNegatedLiteralWords(words, start, number);
    }

    @Override
    public void setSizeInBitsWithinLastWord(int bits) {
        container.setSizeInBitsWithinLastWord(bits);
    }

    @Override
    public void clear() {
        oneBits = 0;
        lastSetBit = -1;
        position = 0;
        container.clear();
    }
}
