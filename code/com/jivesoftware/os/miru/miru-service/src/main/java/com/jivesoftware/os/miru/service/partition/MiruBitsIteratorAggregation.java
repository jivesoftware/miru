package com.jivesoftware.os.miru.service.partition;

import com.googlecode.javaewah.IteratingRLW;

/**
 *
 * @author jonathan
 */
public class MiruBitsIteratorAggregation {

    public static int inplaceAnd(long[] bitmap, IteratingRLW i) {
        int pos = 0;
        long s;
        while ((s = i.size()) > 0) {
            if (pos + s < bitmap.length) {
                final int runLen = (int) i.getRunningLength();
                if (!i.getRunningBit()) {
                    for (int k = pos; k < pos + runLen; ++k) {
                        bitmap[k] = 0;
                    }
                }
                pos += runLen;
                final int LR = i.getNumberOfLiteralWords();
                for (int k = 0; k < LR; ++k) {
                    bitmap[pos++] &= i.getLiteralWordAt(k);
                }
                if (!i.next()) {
                    return pos;
                }
            } else {
                int howmany = bitmap.length - pos;
                int runLen = (int) i.getRunningLength();
                if (pos + runLen > bitmap.length) {
                    if (!i.getRunningBit()) {
                        for (int k = pos; k < bitmap.length; ++k) {
                            bitmap[k] = 0;
                        }
                    }
                    i.discardFirstWords(howmany);
                    return bitmap.length;
                }
                if (!i.getRunningBit()) {
                    for (int k = pos; k < pos + runLen; ++k) {
                        bitmap[k] = 0;
                    }
                }
                pos += runLen;
                for (int k = 0; pos < bitmap.length; ++k) {
                    bitmap[pos++] &= i.getLiteralWordAt(k);
                }
                i.discardFirstWords(howmany);
                return pos;
            }
        }
        return pos;
    }

    public static int inplaceNAnd(long[] bitmap, IteratingRLW i) {
        int pos = 0;
        long s;
        while ((s = i.size()) > 0) {
            if (pos + s < bitmap.length) {
                final int runLen = (int) i.getRunningLength();
                if (i.getRunningBit()) {
                    for (int k = pos; k < pos + runLen; ++k) {
                        bitmap[k] = 0;
                    }
                }
                pos += runLen;
                final int LR = i.getNumberOfLiteralWords();
                for (int k = 0; k < LR; ++k) {
                    bitmap[pos] = ~(bitmap[pos] & i.getLiteralWordAt(k));
                    pos++;
                }
                if (!i.next()) {
                    return pos;
                }
            } else {
                int howmany = bitmap.length - pos;
                int L = (int) i.getRunningLength();
                if (pos + L > bitmap.length) {
                    if (!i.getRunningBit()) {
                        for (int k = pos; k < bitmap.length; ++k) {
                            bitmap[k] = ~0;
                        }
                    }
                    i.discardFirstWords(howmany);
                    return bitmap.length;
                }
                if (i.getRunningBit()) {
                    for (int k = pos; k < pos + L; ++k) {
                        bitmap[k] = 0;
                    }
                }
                pos += L;
                for (int k = 0; pos < bitmap.length; ++k) {
                    bitmap[pos] = ~(bitmap[pos++] & i.getLiteralWordAt(k));
                }
                i.discardFirstWords(howmany);
                return pos;
            }
        }
        return pos;
    }

    public static int inplaceOr(long[] bitmap, IteratingRLW i) {

        int pos = 0;
        long s;
        while ((s = i.size()) > 0) {
            if (pos + s < bitmap.length) {
                final int L = (int) i.getRunningLength();
                if (i.getRunningBit()) {
                    java.util.Arrays.fill(bitmap, pos, pos + L, ~0l);
                }
                pos += L;
                final int LR = i.getNumberOfLiteralWords();

                for (int k = 0; k < LR; ++k) {
                    bitmap[pos++] |= i.getLiteralWordAt(k);
                }
                if (!i.next()) {
                    return pos;
                }
            } else {
                int howmany = bitmap.length - pos;
                int L = (int) i.getRunningLength();

                if (pos + L > bitmap.length) {
                    if (i.getRunningBit()) {
                        java.util.Arrays.fill(bitmap, pos, bitmap.length, ~0l);
                    }
                    i.discardFirstWords(howmany);
                    return bitmap.length;
                }
                if (i.getRunningBit()) {
                    java.util.Arrays.fill(bitmap, pos, pos + L, ~0l);
                }
                pos += L;
                for (int k = 0; pos < bitmap.length; ++k) {
                    bitmap[pos++] |= i.getLiteralWordAt(k);
                }
                i.discardFirstWords(howmany);
                return pos;
            }
        }
        return pos;
    }

    public static int inplaceNOr(long[] bitmap, IteratingRLW i) {
        int pos = 0;
        long s;
        while ((s = i.size()) > 0) {
            if (pos + s < bitmap.length) {
                final int L = (int) i.getRunningLength();
                if (!i.getRunningBit()) {
                    for (int k = pos; k < pos + L; ++k) {
                        bitmap[k] = 0;
                    }
                }
                pos += L;
                final int LR = i.getNumberOfLiteralWords();
                for (int k = 0; k < LR; ++k) {
                    bitmap[pos++] &= (~i.getLiteralWordAt(k));
                }
                if (!i.next()) {
                    return pos;
                }
            } else {
                int howmany = bitmap.length - pos;
                int L = (int) i.getRunningLength();
                if (pos + L > bitmap.length) {
                    if (!i.getRunningBit()) {
                        for (int k = pos; k < bitmap.length; ++k) {
                            bitmap[k] = 0;
                        }
                    }
                    i.discardFirstWords(howmany);
                    return bitmap.length;
                }
                if (!i.getRunningBit()) {
                    for (int k = pos; k < pos + L; ++k) {
                        bitmap[k] = 0;
                    }
                }
                pos += L;
                for (int k = 0; pos < bitmap.length; ++k) {
                    bitmap[pos++] &= (~i.getLiteralWordAt(k));
                }
                i.discardFirstWords(howmany);
                return pos;
            }
        }
        return pos;
    }

    protected static int inplaceXOr(long[] bitmap, IteratingRLW i) {
        int pos = 0;
        long s;
        while ((s = i.size()) > 0) {
            if (pos + s < bitmap.length) {
                final int L = (int) i.getRunningLength();
                if (i.getRunningBit()) {
                    for (int k = pos; k < pos + L; ++k) {
                        bitmap[k] = ~bitmap[k];
                    }
                }
                pos += L;
                final int LR = i.getNumberOfLiteralWords();
                for (int k = 0; k < LR; ++k) {
                    bitmap[pos++] ^= i.getLiteralWordAt(k);
                }
                if (!i.next()) {
                    return pos;
                }
            } else {
                int howmany = bitmap.length - pos;
                int L = (int) i.getRunningLength();
                if (pos + L > bitmap.length) {
                    if (i.getRunningBit()) {
                        for (int k = pos; k < bitmap.length; ++k) {
                            bitmap[k] = ~bitmap[k];
                        }
                    }
                    i.discardFirstWords(howmany);
                    return bitmap.length;
                }
                if (i.getRunningBit()) {
                    for (int k = pos; k < pos + L; ++k) {
                        bitmap[k] = ~bitmap[k];
                    }
                }
                pos += L;
                for (int k = 0; pos < bitmap.length; ++k) {
                    bitmap[pos++] ^= i.getLiteralWordAt(k);
                }
                i.discardFirstWords(howmany);
                return pos;
            }
        }
        return pos;
    }

    private MiruBitsIteratorAggregation() {
    }

}
