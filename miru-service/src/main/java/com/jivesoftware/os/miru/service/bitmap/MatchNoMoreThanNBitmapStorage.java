package com.jivesoftware.os.miru.service.bitmap;

import com.googlecode.javaewah.BitmapStorage;
import com.googlecode.javaewah.EWAHCompressedBitmap;

public class MatchNoMoreThanNBitmapStorage implements BitmapStorage {

    private static final int oneLessBit = EWAHCompressedBitmap.WORD_IN_BITS - 1;
    private final EWAHCompressedBitmap container;
    private final int n;

    private int position;

    public MatchNoMoreThanNBitmapStorage(EWAHCompressedBitmap container, int n) {
        this.container = container;
        this.n = n;
    }

    @Override
    public void addWord(long word) {
        if (position >= n) {
            return;
        }

        if ((position + EWAHCompressedBitmap.WORD_IN_BITS) > n) {
            int bitsToAdd = n - position;
            for (int i = 0; i < (EWAHCompressedBitmap.WORD_IN_BITS - bitsToAdd); i++) {
                word &= ~(1l << oneLessBit - i);
            }
            position += bitsToAdd;
            container.addWord(word, bitsToAdd);
        } else {
            position += EWAHCompressedBitmap.WORD_IN_BITS;
            container.addWord(word);
        }
    }

    @Override
    public void addStreamOfLiteralWords(long[] words, int start, int number) {
        if (position >= n) {
            return;
        }

        for (int i = start; i < start + number; i++) {
            int positionBefore = position;
            addWord(words[i]);
            if (positionBefore == position) {
                break;
            }
        }
    }

    @Override
    public void addStreamOfEmptyWords(boolean v, long number) {
        if (position >= n || number == 0) {
            return;
        }

        int wordsRemaining = (n - position) / EWAHCompressedBitmap.WORD_IN_BITS;
        if (number > wordsRemaining) {
            position += wordsRemaining * EWAHCompressedBitmap.WORD_IN_BITS;
            container.addStreamOfEmptyWords(v, wordsRemaining);

            int bitsRemaining = n - position;
            if (bitsRemaining > 0) {
                long word = 0;
                if (v) {
                    for (int i = 0; i < bitsRemaining; i++) {
                        word |= (1l << i);
                    }
                }
                position += bitsRemaining;
                container.addWord(word, bitsRemaining);
            }
        } else {
            position += number * EWAHCompressedBitmap.WORD_IN_BITS;
            container.addStreamOfEmptyWords(v, number);
        }
    }

    @Override
    public void addStreamOfNegatedLiteralWords(long[] words, int start, int number) {
        if (position >= n) {
            return;
        }

        for (int i = start; i < start + number; i++) {
            long d = ~words[i];
            int positionBefore = position;
            addWord(d);
            if (positionBefore == position) {
                break;
            }
        }
    }

    @Override
    public void setSizeInBitsWithinLastWord(int bits) {
        if (bits > n) {
            bits = n;
        }
        position = bits + 1;
        container.setSizeInBitsWithinLastWord(bits);
    }

    @Override
    public void clear() {
        position = 0;
        container.clear();
    }
}
