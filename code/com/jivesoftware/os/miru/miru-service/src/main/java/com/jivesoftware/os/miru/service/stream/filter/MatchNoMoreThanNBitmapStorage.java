package com.jivesoftware.os.miru.service.stream.filter;

import com.googlecode.javaewah.BitmapStorage;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import java.nio.LongBuffer;

public class MatchNoMoreThanNBitmapStorage implements BitmapStorage {

    private static final int oneLessBit = EWAHCompressedBitmap.wordinbits - 1;
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

        if ((position + EWAHCompressedBitmap.wordinbits) > n) {
            int bitsToAdd = n - position;
            for (int i = 0; i < (EWAHCompressedBitmap.wordinbits - bitsToAdd); i++) {
                word &= ~(1l << oneLessBit - i);
            }
            position += bitsToAdd;
            container.addWord(word, bitsToAdd);
        } else {
            position += EWAHCompressedBitmap.wordinbits;
            container.addWord(word);
        }
    }

    @Override
    public void addStreamOfLiteralWords(LongBuffer words, int start, int number) {
        if (position >= n) {
            return;
        }

        for (int i = start; i < start + number; i++) {
            int positionBefore = position;
            addWord(words.get(i));
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

        int wordsRemaining = (n - position) / EWAHCompressedBitmap.wordinbits;
        if (number > wordsRemaining) {
            position += wordsRemaining * EWAHCompressedBitmap.wordinbits;
            container.addStreamOfEmptyWords(v, wordsRemaining);

            int bitsRemaining = n - position;
            long word = 0;
            if (v) {
                for (int i = 0; i < bitsRemaining; i++) {
                    word |= (1l << i);
                }
            }
            position += bitsRemaining;
            container.addWord(word, bitsRemaining);
        } else {
            position += number * EWAHCompressedBitmap.wordinbits;
            container.addStreamOfEmptyWords(v, number);
        }
    }

    @Override
    public void addStreamOfNegatedLiteralWords(LongBuffer words, int start, int number) {
        if (position >= n) {
            return;
        }

        for (int i = start; i < start + number; i++) {
            long d = ~words.get(i);
            int positionBefore = position;
            addWord(d);
            if (positionBefore == position) {
                break;
            }
        }
    }

    @Override
    public void setSizeInBits(int bits) {
        if (bits > n) {
            bits = n;
        }
        position = bits + 1;
        container.setSizeInBits(bits);
    }

}
