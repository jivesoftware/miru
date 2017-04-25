package com.jivesoftware.os.miru.catwalk.deployable;

import com.jivesoftware.os.amza.api.PartitionClient.KeyValueFilter;
import com.jivesoftware.os.amza.api.filer.HeapFiler;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.stream.KeyValueStream;
import java.io.IOException;
import java.util.Arrays;

import static com.jivesoftware.os.amza.api.filer.UIO.readInt;

/**
 *
 */
public class CatwalkKeyValueFilter implements KeyValueFilter {

    private final float gatherMinFeatureScore;
    private final int topN;

    public CatwalkKeyValueFilter(float gatherMinFeatureScore, int topN) {
        this.gatherMinFeatureScore = gatherMinFeatureScore;
        this.topN = topN;
    }

    @Override
    public boolean filter(byte[] prefix,
        byte[] key,
        byte[] value,
        long timestamp,
        boolean tombstoned,
        long version,
        KeyValueStream stream) throws Exception {

        if (value != null) {
            HeapFiler in = HeapFiler.fromBytes(value, value.length);
            HeapFiler out = new HeapFiler((int) in.length());
            filterRewrite(in, out);
            return stream.stream(prefix, key, out.getBytes(), timestamp, tombstoned, version);
        } else {
            return stream.stream(prefix, key, value, timestamp, tombstoned, version);
        }
    }

    private void filterRewrite(HeapFiler in, HeapFiler out) throws IOException {

        byte version = UIO.readByte(in, "version");
        UIO.writeByte(out, version, "version");
        if (version < 1 || version > 3) {
            throw new IllegalStateException("Unexpected version " + version);
        }

        UIO.writeByte(out, UIO.readByte(in, "partitionIsClosed"), "partitionIsClosed");

        byte[] lengthBuffer = new byte[8];

        if (version < 2) {
            int modelCountsLength = readInt(in, "modelCountsLength", lengthBuffer);
            UIO.writeInt(out, modelCountsLength, "modelCountsLength", lengthBuffer);

            for (int i = 0; i < modelCountsLength; i++) {
                UIO.writeLong(out, UIO.readLong(in, "modelCount", lengthBuffer), "modelCount", lengthBuffer);
            }
        } else {
            UIO.writeLong(out, UIO.readLong(in, "modelCount", lengthBuffer), "modelCount", lengthBuffer);
        }

        UIO.writeLong(out, UIO.readLong(in, "totalCount", lengthBuffer), "totalCount", lengthBuffer);

        int scoresLength = readInt(in, "scoresLength", lengthBuffer);
        long scoresLengthFp = out.getFilePointer();
        UIO.writeInt(out, 0, "scoresLength", lengthBuffer); // rewrite later

        int[] fp = null;
        if (topN > 0 && scoresLength > topN) {

            float[] scores = new float[scoresLength];
            fp = new int[scoresLength];

            for (int i = 0; i < scoresLength; i++) {
                fp[i] = (int) in.getFilePointer();
                int ts = UIO.readInt(in, "termsLength", lengthBuffer);
                for (int j = 0; j < ts; j++) {
                    int termLength = UIO.readInt(in, "termLength", lengthBuffer);
                    in.skip(termLength);
                }

                long maxNumerator = 0;
                if (version < 3) {
                    maxNumerator = UIO.readLong(in, "numerator", lengthBuffer);
                } else {
                    int numeratorCount = UIO.readByte(in, "numeratorCount");
                    for (int j = 0; j < numeratorCount; j++) {
                        maxNumerator = Math.max(UIO.readLong(in, "numerator", lengthBuffer), maxNumerator);
                    }
                }
                long denominator = UIO.readLong(in, "denominator", lengthBuffer);
                UIO.readInt(in, "numPartitions", lengthBuffer);
                if (maxNumerator > 0) {
                    float s = (float) maxNumerator / denominator;
                    if (s > gatherMinFeatureScore) {
                        scores[i] = s;
                    } else {
                        scores[i] = -1;
                    }
                } else {
                    scores[i] = -1;
                }
            }

            sortFI(scores, fp, 0, scoresLength);
            Arrays.fill(fp, 0, scoresLength - topN, Integer.MAX_VALUE);
            Arrays.sort(fp);
        }

        int accepted = 0;
        for (int i = 0; i < scoresLength; i++) {

            if (fp != null) {
                if (fp[i] == Integer.MAX_VALUE) {
                    break;
                } else {
                    in.seek(fp[i]);
                }
            }

            byte[][] terms = new byte[UIO.readInt(in, "termsLength", lengthBuffer)][];
            for (int j = 0; j < terms.length; j++) {
                terms[j] = UIO.readByteArray(in, "term", lengthBuffer);
            }
            long[] numerators;
            long maxNumerator = 0;
            if (version < 3) {
                numerators = new long[] { UIO.readLong(in, "numerator", lengthBuffer) };
            } else {
                int numeratorCount = UIO.readByte(in, "numeratorCount");
                numerators = new long[numeratorCount];
                for (int j = 0; j < numeratorCount; j++) {
                    numerators[j] = UIO.readLong(in, "numerator", lengthBuffer);
                    maxNumerator = Math.max(numerators[j], maxNumerator);
                }
            }
            long denominator = UIO.readLong(in, "denominator", lengthBuffer);
            int numPartitions = UIO.readInt(in, "numPartitions", lengthBuffer);

            if (maxNumerator > 0) {
                float s = (float) maxNumerator / denominator;
                if (s > gatherMinFeatureScore) {
                    accepted++;

                    UIO.writeInt(out, terms.length, "termsLength", lengthBuffer);
                    for (int j = 0; j < terms.length; j++) {
                        UIO.writeByteArray(out, terms[j], "term", lengthBuffer);
                    }

                    if (version < 3) {
                        UIO.writeLong(out, numerators[0], "numerator", lengthBuffer);
                    } else {
                        UIO.writeByte(out, (byte) numerators.length, "numeratorCount");
                        for (int j = 0; j < numerators.length; j++) {
                            UIO.writeLong(out, numerators[j], "numerator", lengthBuffer);
                        }
                    }

                    UIO.writeLong(out, denominator, "denominator", lengthBuffer);
                    UIO.writeInt(out, numPartitions, "numPartitions", lengthBuffer);
                }
            }
        }

        long tailFp = out.getFilePointer();
        out.seek(scoresLengthFp);
        UIO.writeInt(out, accepted, "scoresLength", lengthBuffer);
        out.seek(tailFp);

        UIO.writeLong(out, UIO.readLong(in, "smallestTimestamp", lengthBuffer), "smallestTimestamp", lengthBuffer);
        UIO.writeLong(out, UIO.readLong(in, "largestTimestamp", lengthBuffer), "largestTimestamp", lengthBuffer);
    }

    private static void sortFI(float[] x, int[] keys, int off, int len) {
        // Insertion sort on smallest arrays
        if (len < 7) {
            for (int i = off; i < len + off; i++) {
                for (int j = i; j > off && x[j - 1] > x[j]; j--) {
                    swapFI(x, keys, j, j - 1);
                }
            }
            return;
        }

        // Choose a partition element, v
        int m = off + (len >> 1);       // Small arrays, middle element
        if (len > 7) {
            int l = off;
            int n = off + len - 1;
            if (len > 40) {        // Big arrays, pseudomedian of 9
                int s = len / 8;
                l = med3F(x, l, l + s, l + 2 * s);
                m = med3F(x, m - s, m, m + s);
                n = med3F(x, n - 2 * s, n - s, n);
            }
            m = med3F(x, l, m, n); // Mid-size, med of 3
        }
        float v = x[m];

        // Establish Invariant: v* (<v)* (>v)* v*
        int a = off, b = a, c = off + len - 1, d = c;
        while (true) {
            while (b <= c && x[b] <= v) {
                if (x[b] == v) {
                    swapFI(x, keys, a++, b);
                }
                b++;
            }
            while (c >= b && x[c] >= v) {
                if (x[c] == v) {
                    swapFI(x, keys, c, d--);
                }
                c--;
            }
            if (b > c) {
                break;
            }
            swapFI(x, keys, b++, c--);
        }

        // Swap partition elements back to middle
        int s, n = off + len;
        s = Math.min(a - off, b - a);
        vecswapFI(x, keys, off, b - s, s);
        s = Math.min(d - c, n - d - 1);
        vecswapFI(x, keys, b, n - s, s);

        // Recursively sort non-partition-elements
        if ((s = b - a) > 1) {
            sortFI(x, keys, off, s);
        }
        if ((s = d - c) > 1) {
            sortFI(x, keys, n - s, s);
        }
    }

    /**
     * Swaps x[a] with x[b].
     */
    private static void swapFI(float x[], int[] keys, int a, int b) {
        float t = x[a];
        x[a] = x[b];
        x[b] = t;

        int l = keys[a];
        keys[a] = keys[b];
        keys[b] = l;

    }

    /**
     * Swaps x[a .. (a+n-1)] with x[b .. (b+n-1)].
     */
    private static void vecswapFI(float x[], int[] keys, int a, int b, int n) {
        for (int i = 0; i < n; i++, a++, b++) {
            swapFI(x, keys, a, b);
        }
    }

    private static int med3F(float x[], int a, int b, int c) {
        return (x[a] < x[b] ? (x[b] < x[c] ? b : x[a] < x[c] ? c : a) : (x[b] > x[c] ? b : x[a] > x[c] ? c : a));
    }

}
